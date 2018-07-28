package main

import (
	"bytes"
	"fmt"
	"hash"
	"io/ioutil"
	"net"
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
	conn.conn.Close()
	conn.connected = false
}

func (conn *tsdbConn) startReadAsync() {
	buffer, err := ioutil.ReadAll(conn.conn)
	if err != nil {
		fmt.Println("Read error", err)
	} else {
		fmt.Println("Database returned error:", string(buffer))
	}
	conn.writeChan <- nil // To stop the goroutine
	conn.conn.Close()
	conn.connected = false
	conn.reconnectChan <- 0
}

func (conn *tsdbConn) run() {
	fmt.Println("RUN")
	for {
		buf := <-conn.writeChan
		if buf == nil {
			break
		}
		err := conn.write(buf)
		if err != nil {
			fmt.Println("TSDB write error", err)
			break
		} else {
			fmt.Println("Write is OK")
		}
	}
	fmt.Println("RUN...Exit")
}

func (conn *tsdbConn) reconnect() {
	for {
		ix := <-conn.reconnectChan
		if ix < 0 {
			fmt.Println("Reconnection job stopped")
			break
		}
		fmt.Println("Connection attempt", ix)
		if conn.connected {
			fmt.Println("Reconnection triggered")
			conn.conn.Close()
		} else {
			fmt.Println("Connection triggered")
		}
		err := conn.connect()
		if err != nil {
			fmt.Println("TSDB connection error", err)
			conn.reconnectChan <- (ix + 1)
		} else {
			fmt.Println("Connection attempt successful")
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
	pool []tsdbConn
	hash hash.Hash32
}

func (conn tsdbConnPool) Connect(addr string, connectTimeout, writeTimeout time.Duration) error {
	return nil
}

func main() {
	tsdb := createTsdb("127.0.0.1:8282", time.Second*2, time.Second*10)
	var payload bytes.Buffer
	payload.WriteString("hello")
	tsdb.Write(payload.Bytes())
	time.Sleep(1000 * time.Second)
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
