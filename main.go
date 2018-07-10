package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"

	"github.com/prometheus/prometheus/prompb"
)

func main() {
	tsdb, err := net.Dial("tcp", "127.0.0.1:8282")
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

		for _, ts := range req.Timeseries {
			var tags bytes.Buffer
			var metric string
			for _, l := range ts.Labels {
				if l.Name != "__name__" {
					tags.WriteString(fmt.Sprintf(" %s=%s", l.Name, l.Value))
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
					tsdb.Write(payload.Bytes())
				}
			}
		}
	})

	http.ListenAndServe("127.0.0.1:9201", nil)
}
