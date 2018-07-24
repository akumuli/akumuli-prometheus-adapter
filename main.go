package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"

	"github.com/prometheus/prometheus/prompb"
)

type tsdbConn struct {
	conn         net.Conn
	writeTimeout time.Duration
	writeChan    chan bytes.Buffer
	errorChan    chan error
}

func (conn tsdbConn) run() {
	for msg := range conn.writeChan {
		err := conn.write(msg)
		if err != nil {
			conn.errorChan <- err
		}
	}
}

func (conn tsdbConn) connect(addr string, connectTimeout, writeTimeout time.Duration) error {
	conn.writeTimeout = writeTimeout
	c, err := net.DialTimeout("tcp", addr, connectTimeout)
	if err != nil {
		conn.conn = c
		conn.writeChan = make(chan bytes.Buffer, 100)
		conn.errorChan = make(chan error)
		go conn.run()
	}
	return err
}

func (conn tsdbConn) write(data bytes.Buffer) error {
	deadline := time.Now()
	deadline.Add(conn.writeTimeout)
	_, err := conn.conn.Write(data.Bytes())
	return err
}

func (conn tsdbConn) Write(data bytes.Buffer) {
	conn.writeChan <- data
}

func main() {
	var tsdb tsdbConn
	err := tsdb.connect("127.0.0.1:8282", time.Second*10, time.Second*10)
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
}
