package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

func escapeSpaces(name string) string {
	return strings.Replace(name, " ", "\\ ", -1)
}

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
		if sz < 2 {
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

type tsdbClient struct {
}

type tsdbQueryRange struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type tsdbQuery struct {
	MetricName string            `json:"select"`
	TimeRange  tsdbQueryRange    `json:"range"`
	Where      map[string]string `json:"where"`
}

func buildCommand(q *prompb.Query) ([]byte, error) {
	var query tsdbQuery
	query.Where = make(map[string]string)
	for _, m := range q.Matchers {
		if m.Name == model.MetricNameLabel {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				query.MetricName = escapeSpaces(m.Value)
			default:
				return nil, fmt.Errorf("non-equal or regex matchers are not supported on the metric name yet")
			}
			continue
		}

		switch m.Type {
		case prompb.LabelMatcher_EQ:
			query.Where[m.Name] = escapeSpaces(m.Value)
		default:
			return nil, fmt.Errorf("unknown match type %v", m.Type)
		}
	}
	query.TimeRange.From = fmt.Sprintf("%v", q.StartTimestampMs*1000000)
	query.TimeRange.To = fmt.Sprintf("%v", q.EndTimestampMs*1000000)

	return json.Marshal(query)
}

func toLabelPairs(series string) []*prompb.Label {
	items := strings.Split(series, " ")
	pairs := make([]*prompb.Label, 0, len(items))
	for _, token := range items[1:] {
		kv := strings.Split(token, "=")
		if len(kv) != 2 {
			continue
		}
		pairs = append(pairs, &prompb.Label{
			Name:  kv[0],
			Value: kv[1],
		})
	}
	pairs = append(pairs, &prompb.Label{
		Name:  model.MetricNameLabel,
		Value: items[0],
	})
	return pairs
}

func appendResult(labelsToSeries map[string]*prompb.TimeSeries,
	name string,
	timestamp time.Time,
	value float64) error {

	tss, ok := labelsToSeries[name]
	if !ok {
		tss = &prompb.TimeSeries{
			Labels: toLabelPairs(name),
		}
		labelsToSeries[name] = tss
	}
	tss.Samples = append(tss.Samples, &prompb.Sample{
		Timestamp: timestamp.UnixNano() / 1000000,
		Value:     value,
	})

	return nil
}

func (c *tsdbClient) Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	series := map[string]*prompb.TimeSeries{}
	for _, query := range req.GetQueries() {
		tsdbq, err := buildCommand(query)
		if err != nil {
			// TODO: remove
			fmt.Println(err.Error())
			return nil, err
		}
		resp, httperr := http.Post("http://localhost:8181/api/query", "application/json", bytes.NewReader(tsdbq))
		if httperr != nil {
			// TODO: remove
			fmt.Println(httperr.Error())
			return nil, httperr
		}
		defer resp.Body.Close()
		output, readerr := ioutil.ReadAll(resp.Body)
		if readerr != nil {
			// TODO: remove
			fmt.Println(readerr.Error())
			return nil, readerr
		}

		// Parse output
		lines := strings.Split(string(output), "\r\n")
		var name string
		var timestamp time.Time
		var value float64
		layout := "20060102T150405.999999999"
		for ixline, line := range lines {
			if len(line) == 0 {
				continue
			}
			switch ixline % 3 {
			case 0:
				// Parse series name
				name = line[1:len(line)]
			case 1:
				// Parse timestamp
				timestamp, err = time.Parse(layout, line[1:len(line)])
				if err != nil {
					return nil, err
				}
			case 2:
				// Parse float value
				value, err = strconv.ParseFloat(line[1:len(line)], 64)
				if err != nil {
					return nil, err
				}
				err = appendResult(series, name, timestamp, value)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	// Build response
	resp := prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{Timeseries: make([]*prompb.TimeSeries, 0, len(series))},
		},
	}
	for _, ts := range series {
		resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, ts)
	}
	return &resp, nil
}

type config struct {
	writeTimeout      time.Duration
	reconnectInterval time.Duration
	idleTime          time.Duration
	tsdbAddress       string
	tsdbPort          int
}

func main() {
	var cfg config
	flag.DurationVar(&cfg.writeTimeout,
		"write-timeout",
		time.Second*10,
		"TSDB write timeout",
	)

	flag.DurationVar(&cfg.reconnectInterval,
		"reconnect-interval",
		time.Second*5,
		"Reconnect interval",
	)

	flag.DurationVar(&cfg.idleTime,
		"idle-time",
		time.Second*60,
		"Interval after which idle connection should be closed",
	)

	flag.StringVar(&cfg.tsdbAddress,
		"host",
		"localhost",
		"TSDB host address",
	)

	flag.IntVar(&cfg.tsdbPort,
		"port",
		8282,
		"TSDB port number",
	)

	endpoint := fmt.Sprintf("%s:%d", cfg.tsdbAddress, cfg.tsdbPort)

	tsdb := createTsdbPool(endpoint,
		cfg.reconnectInterval, cfg.writeTimeout, cfg.idleTime)

	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("Read request")

		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Println("Can't read response, error:", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			log.Println("Can't decode response, error:", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			log.Println("Can't unmarshal response, error:", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var client tsdbClient
		resp, err := client.Read(&req)
		if err != nil {
			log.Println("Can't generate response, error:", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := proto.Marshal(resp)
		if err != nil {
			log.Println("Can't marshal response, error:", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			log.Println("Can't send response, error:", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

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

		for _, ts := range req.Timeseries {
			var tags bytes.Buffer
			var metric string
			for _, l := range ts.Labels {
				if l.Name != "__name__" {
					sval := escapeSpaces(l.Value)
					sname := escapeSpaces(l.Name)
					tags.WriteString(fmt.Sprintf(" %s=%s", sname, sval))
				} else {
					metric = escapeSpaces(l.Value)
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
					tsdb.Write(r.RemoteAddr, payload.Bytes())
				}
			}
		}
	})

	http.ListenAndServe("127.0.0.1:9201", nil)
}
