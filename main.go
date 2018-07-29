package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type tsdbConn struct {
	conn           net.Conn
	writeTimeout   time.Duration
	connectTimeout time.Duration
	connected      bool
	writeChan      chan []byte
	reconnectChan  chan int
	addr           string
}

// Create new connection
func createTsdb(addr string, connectTimeout, writeTimeout time.Duration) *tsdbConn {
	conn := new(tsdbConn)
	conn.writeTimeout = writeTimeout
	conn.connectTimeout = connectTimeout
	conn.writeChan = make(chan []byte, 100)
	conn.reconnectChan = make(chan int, 10)
	conn.connected = false
	conn.addr = addr
	conn.reconnectChan <- 0
	go conn.reconnect()
	return conn
}

func (conn *tsdbConn) connect() error {
	time.Sleep(conn.connectTimeout)
	c, err := net.Dial("tcp", conn.addr)
	if err == nil {
		conn.conn = c
		conn.connected = true
	} else {
		conn.connected = false
	}
	return err
}

func (conn *tsdbConn) write(data []byte) error {
	deadline := time.Now()
	deadline.Add(conn.writeTimeout)
	_, err := conn.conn.Write(data)
	return err
}

func (conn *tsdbConn) Close() {
	conn.reconnectChan <- -1
	conn.writeChan <- nil
	if conn.connected {
		conn.conn.Close()
		conn.connected = false
	}
}

func (conn *tsdbConn) startReadAsync() {
	buffer, err := ioutil.ReadAll(conn.conn)
	if err != nil {
		log.Println("Read error", err)
	} else {
		log.Println("Database returned error:", string(buffer))
	}
	conn.writeChan <- nil // To stop the goroutine
	conn.conn.Close()
	conn.connected = false
	conn.reconnectChan <- 0
}

func (conn *tsdbConn) run() {
	for {
		buf := <-conn.writeChan
		if buf == nil {
			break
		}
		err := conn.write(buf)
		if err != nil {
			log.Println("TSDB write error:", err)
			break
		}
	}
}

func (conn *tsdbConn) reconnect() {
	for {
		ix := <-conn.reconnectChan
		if ix < 0 {
			log.Println("Reconnection job stopping")
			break
		}
		if conn.connected {
			conn.conn.Close()
			conn.connected = false
		}
		err := conn.connect()
		if err != nil {
			log.Println("TSDB connection error", err)
			conn.reconnectChan <- (ix + 1)
		} else {
			log.Println("Connection attempt successful")
			go conn.run()
			go conn.startReadAsync()
		}
	}
}

func (conn *tsdbConn) Write(data []byte) {
	conn.writeChan <- data
}

// TSDB connection pool with affinity
type tsdbConnPool struct {
	pool           map[string]*tsdbConn
	targetAddr     string
	writeTimeout   time.Duration
	connectTimeout time.Duration
	watchdog       *time.Ticker
	rwlock         sync.RWMutex
}

func createTsdbPool(targetAddr string, connectTimeout, writeTimeout, watchTimeout time.Duration) *tsdbConnPool {
	tsdb := new(tsdbConnPool)
	tsdb.pool = make(map[string]*tsdbConn)
	tsdb.connectTimeout = connectTimeout
	tsdb.writeTimeout = writeTimeout
	tsdb.targetAddr = targetAddr
	tsdb.watchdog = time.NewTicker(watchTimeout)
	go tsdb.runWatchdog()
	return tsdb
}

func (tsdb *tsdbConnPool) runWatchdog() {
	for range tsdb.watchdog.C {
		// Chose item from the pool randomly
		tsdb.rwlock.RLock()
		sz := len(tsdb.pool)
		tsdb.rwlock.RUnlock()
		if sz == 0 { // TODO: replace with 'if sz < 2 {'
			continue
		}
		ix := rand.Int() % sz
		it := 0
		tsdb.rwlock.Lock()
		for key, val := range tsdb.pool {
			if it == ix {
				log.Println("Watchdog invoked. Stopping", key)
				val.Close()
				delete(tsdb.pool, key)
				break
			}
			it++
		}
		tsdb.rwlock.Unlock()
	}
}

func (tsdb *tsdbConnPool) Write(source string, buf []byte) {
	tsdb.rwlock.RLock()
	conn, ok := tsdb.pool[source]
	tsdb.rwlock.RUnlock()
	if !ok {
		// It is possible that this branch will be executed twice for the same
		// key, but that's OK. Normally, it won't be executed often. And the
		// method will be called with the same source by the same client. I
		// expect that parallel calls to this method will always have different
		// 'source' values.
		tsdb.rwlock.Lock()
		conn = createTsdb(tsdb.targetAddr, tsdb.connectTimeout, tsdb.writeTimeout)
		tsdb.pool[source] = conn
		tsdb.rwlock.Unlock()
	}
	conn.Write(buf)
}

func (tsdb *tsdbConnPool) Close() {
	tsdb.rwlock.Lock()
	for _, val := range tsdb.pool {
		val.Close()
	}
	tsdb.watchdog.Stop()
	tsdb.rwlock.Unlock()
}

func main() {
	tsdb := createTsdbPool("127.0.0.1:8282",
		time.Second*2, time.Second*10, time.Second*20)
	var payload bytes.Buffer
	payload.WriteString("hello")
	tsdb.Write("127.0.0.1:8181", payload.Bytes())
	// reader := bufio.NewReader(os.Stdin)
	// text, _ := reader.ReadString('\n')
	// payload.Reset()
	// payload.WriteString(text)
	// tsdb.Write("127.0.0.1:0303", payload.Bytes())
	// Test stopping
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	tsdb.Close()
	fmt.Println("TSDB Stopped")
	/*
		var tsdb tsdbConn
		err := tsdb.Connect("127.0.0.1:8282", time.Second*10, time.Second*10)
		if err != nil {
			fmt.Println("Can't connect to TSDB", err)
			return
		}

		http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {

			compressed, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			reqBuf, err := snappy.Decode(nil, compressed)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			var req prompb.WriteRequest
			if err := proto.Unmarshal(reqBuf, &req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			fmt.Println("remote addr: ", r.RemoteAddr)

			for _, ts := range req.Timeseries {
				var tags bytes.Buffer
				var metric string
				for _, l := range ts.Labels {
					if l.Name != "__name__" {
						if strings.Count(l.Value, " ") == 0 {
							tags.WriteString(fmt.Sprintf(" %s=%s", l.Name, l.Value))
						}
					} else {
						metric = l.Value
					}
				}
				sname := fmt.Sprintf("+%s%s\r\n", metric, tags.String())
				var payload bytes.Buffer
				for _, s := range ts.Samples {
					if !math.IsNaN(s.Value) {
						payload.WriteString(sname)
						dt := time.Unix(s.Timestamp/1000, (s.Timestamp%1000)*1000000).UTC()
						payload.WriteString(fmt.Sprintf("+%s\r\n", dt.Format("20060102T150405")))
						payload.WriteString(fmt.Sprintf("+%.17g\r\n", s.Value))
						tsdb.write(payload)
					}
				}
			}
		})

		http.ListenAndServe("127.0.0.1:9201", nil)
	*/
}
