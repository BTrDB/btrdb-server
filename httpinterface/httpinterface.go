package httpinterface

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"fmt"
	"github.com/SoftwareDefinedBuildings/quasar"
	"github.com/SoftwareDefinedBuildings/quasar/qtree"
	"github.com/bmizerany/pat"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)

func doError(w http.ResponseWriter, e string) {
	log.Printf("returning error %v", e)
	w.WriteHeader(400)
	w.Write([]byte(e))
}

func parseInt(input string, minval int64, maxval int64) (int64, bool, string) {
	rv, err := strconv.ParseInt(input, 10, 64)
	if rv < minval || rv >= maxval {
		return 0, false, "out of acceptable range"
	}
	if err != nil {
		return 0, false, "malformed"
	}
	return rv, true, ""
}
func request_get_VRANGE(q *quasar.Quasar, w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	ids := r.Form.Get(":uuid")
	id := uuid.Parse(ids)
	if id == nil {
		log.Printf("ids: '%v'", ids)
		doError(w, "malformed uuid")
		return
	}
	st, ok, msg := parseInt(r.Form.Get("starttime"), -(16 << 56), (48 << 56))
	if !ok {
		doError(w, "bad start time: "+msg)
		return
	}
	et, ok, msg := parseInt(r.Form.Get("endtime"), -(16 << 56), (48 << 56))
	if !ok {
		doError(w, "bad end time: "+msg)
		return
	}
	if et <= st {
		doError(w, "end time <= start time")
		return
	}
	versions := r.Form.Get("ver")
	if versions == "" {
		versions = "0"
	}
	//Technically this is incorrect, but I doubt we will overflow this
	versioni, ok, msg := parseInt(versions, 0, 1<<63-1)
	version := uint64(versioni)
	if !ok {
		doError(w, "malformed version: "+msg)
		return
	}
	if version == 0 {
		version = quasar.LatestGeneration
	}
	unitoftime := r.Form.Get("unitoftime")
	uot := struct {
		UnitofTime string
	}{unitoftime}
	divisor := int64(1)
	switch unitoftime {
	case "":
		fallthrough
	case "ms":
		divisor = 1000000 //ns to ms
	case "ns":
		divisor = 1
	case "us":
		divisor = 1000 //ns to us
	case "s":
		divisor = 1000000000 //ns to s
	default:
		doError(w, "unitoftime must be 'ns', 'ms', 'us' or 's'")
		return
	}
	if st >= quasar.MaximumTime/divisor ||
		st <= quasar.MinimumTime/divisor {
		doError(w, "start time out of bounds")
		return
	}
	if et >= quasar.MaximumTime/divisor ||
		et <= quasar.MinimumTime/divisor {
		doError(w, "end time out of bounds")
		return
	}
	st *= divisor
	et *= divisor
	pws := r.Form.Get("pw")
	pw := uint8(0)
	if pws != "" {
		pwl, ok, msg := parseInt(pws, 0, 63)
		if !ok {
			doError(w, "bad point width: "+msg)
			return
		}
		if divisor != 1 {
			doError(w, "statistical results require unitoftime=ns")
			return
		}
		pw = uint8(pwl)
	}
	log.Printf("pw %v", pw)
	if pws != "" {
		res, rgen, err := q.QueryStatisticalValues(id, st, et, version, pw)
		if err != nil {
			doError(w, "query error: "+err.Error())
			return
		}
		resf := make([][]interface{}, len(res))
		contents := make([]interface{}, len(res)*6)
		for i := 0; i < len(res); i++ {
			resf[i] = contents[i*6 : (i+1)*6]
			resf[i][0] = res[i].Time / 1000000 //ms since epoch
			resf[i][1] = res[i].Time % 1000000 //nanoseconds left over
			resf[i][2] = res[i].Min
			resf[i][3] = res[i].Mean
			resf[i][4] = res[i].Max
			resf[i][5] = res[i].Count
		}
		rv := []struct {
			Uuid      string `json:"uuid"`
			XReadings [][]interface{}
			Version   uint64 `json:"version"`
		}{
			{id.String(), resf, rgen},
		}
		err = json.NewEncoder(w).Encode(rv)
		if err != nil {
			doError(w, "JSON error: "+err.Error())
			return
		}
		return
	} else {
		res, rgen, err := q.QueryValues(id, st, et, version)
		if err != nil {
			doError(w, "query error: "+err.Error())
			return
		}
		resf := make([][]interface{}, len(res))
		contents := make([]interface{}, len(res)*2)
		for i := 0; i < len(res); i++ {
			resf[i] = contents[i*2 : (i+1)*2]
			resf[i][0] = res[i].Time / divisor
			resf[i][1] = res[i].Val
		}

		//props := struct{Uot string `json:"UnitofTime"`}{"foo"}
		rv := []struct {
			Uuid       string `json:"uuid"`
			Readings   [][]interface{}
			Version    uint64      `json:"version"`
			Properties interface{} `json:"Properties"`
		}{
			{id.String(), resf, rgen, uot},
		}
		err = json.NewEncoder(w).Encode(rv)
		if err != nil {
			doError(w, "JSON error: "+err.Error())
			return
		}
		return
	}
	//res, err := q.
}

type insert_t struct {
	Uuid     string `json:"uuid"`
	Readings [][]interface{}
}

func request_post_INSERT(q *quasar.Quasar, w http.ResponseWriter, r *http.Request) {
	then := time.Now()
	dec := json.NewDecoder(r.Body)
	var ins insert_t
	dec.UseNumber()
	err := dec.Decode(&ins)
	if err != nil {
		doError(w, "malformed quasar HTTP insert")
		return
	}
	id := uuid.Parse(ins.Uuid)
	if id == nil {
		doError(w, "malformed uuid")
		return
	}
	//log.Printf("Got %+v", ins)

	recs := make([]qtree.Record, len(ins.Readings))

	//Check the format of the insert and copy to Record
	for i := 0; i < len(ins.Readings); i++ {
		if len(ins.Readings[i]) != 2 {
			doError(w, fmt.Sprintf("reading %d is malformed", i))
			return
		}
		t, ok, msg := parseInt(string(ins.Readings[i][0].(json.Number)), quasar.MinimumTime, quasar.MaximumTime)
		if !ok {
			doError(w, fmt.Sprintf("reading %d time malformed: %s", i, msg))
			return
		}
		val, err := strconv.ParseFloat(string(ins.Readings[i][1].(json.Number)), 64)
		if err != nil {
			doError(w, fmt.Sprintf("value %d malformed: %s", i, err))
			return
		}
		recs[i].Time = t
		recs[i].Val = val
	}
	q.InsertValues(id, recs)
	//log.Printf("got %+v", recs)
	delta := time.Now().Sub(then)

	w.Write([]byte(fmt.Sprintf("OK %d records, %.2f ms\n", len(recs), float64(delta.Nanoseconds()/1000)/1000)))
}

type SmapReading struct {
	Readings   [][]interface{}
	Properties struct {
		UnitofTime string
	}
	UUID string `json:"uuid"`
}

//From GTF
func processJSON(body io.Reader) ([]SmapReading, error) {
	var reading map[string]*json.RawMessage
	var rv []SmapReading
	dec := json.NewDecoder(body)
	dec.UseNumber()
	err := dec.Decode(&reading)
	if err != nil {
		return nil, err
	}

	for _, v := range reading {
		if v == nil {
			continue
		}
		var sr SmapReading
		buf := bytes.NewBuffer([]byte(*v))
		dec2 := json.NewDecoder(buf)
		dec2.UseNumber()
		err = dec2.Decode(&sr)
		if err != nil {
			return nil, err
		}
		rv = append(rv, sr)
	}
	return rv, nil
}

func request_post_LEGACYINSERT(q *quasar.Quasar, w http.ResponseWriter, r *http.Request) {
	then := time.Now()
	records, err := processJSON(r.Body)
	if err != nil {
		doError(w, "malformed body")
		return
	}
	for _, r := range records {
		if r.UUID == "" {
			continue
		}
		id := uuid.Parse(r.UUID)
		if id == nil {
			doError(w, "malformed uuid")
			return
		}
		recs := make([]qtree.Record, len(r.Readings))
		//Check the format of the insert and copy to Record
		for i := 0; i < len(r.Readings); i++ {
			uot := r.Properties.UnitofTime
			var uotmult int64
			switch uot {
			default:
				fallthrough
			case "s":
				uotmult = 1000000000
			case "ms":
				uotmult = 1000000
			case "us":
				uotmult = 1000
			case "ns":
				uotmult = 1
			}
			if len(r.Readings[i]) != 2 {
				doError(w, fmt.Sprintf("reading %d of record %v is malformed", i, r.UUID))
				return
			}
			t, ok, msg := parseInt(string(r.Readings[i][0].(json.Number)), quasar.MinimumTime, quasar.MaximumTime)
			if !ok {
				doError(w, fmt.Sprintf("reading %d time malformed: %s", i, msg))
				return
			}

			if t >= (quasar.MaximumTime/uotmult) || t <= (quasar.MinimumTime/uotmult) {
				doError(w, fmt.Sprintf("reading %d time out of range", i))
				return
			}
			t *= uotmult
			var val float64
			bval, ok := r.Readings[i][1].(bool)
			if ok {
			  if bval {
			     val = 1
			  } else {
			     val = 0
			  }
			} else {
				val, err = strconv.ParseFloat(string(r.Readings[i][1].(json.Number)), 64)
				if err != nil {
					doError(w, fmt.Sprintf("value %d malformed: %s", i, err))
					return
				}
			}
			recs[i].Time = t
			recs[i].Val = val

		}
		q.InsertValues(id, recs)
	}
	delta := time.Now().Sub(then)
	w.Write([]byte(fmt.Sprintf("OK %.2f ms\n", float64(delta.Nanoseconds()/1000)/1000)))
}
func curry(q *quasar.Quasar,
	f func(*quasar.Quasar, http.ResponseWriter, *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		f(q, w, r)
	}
}
func QuasarServeHTTP(q *quasar.Quasar, addr string) {
	mux := pat.New()
	mux.Get("/data/uuid/:uuid", http.HandlerFunc(curry(q, request_get_VRANGE)))
	//mux.Get("/q/versions", http.HandlerFunc(curry(q, request_get_VERSIONS)))
	//mux.Get("/q/nearest", http.Handler(curry(q, request_get_NEAREST)))
	mux.Post("/data/add/:subkey", http.HandlerFunc(curry(q, request_post_INSERT)))
	mux.Post("/data/legacyadd/:subkey", http.HandlerFunc(curry(q, request_post_LEGACYINSERT)))
	//mux.Post("/q/:uuid/v", curry(q, p
	log.Printf("serving http on %v", addr)
	err := http.ListenAndServe(addr, mux)
	log.Panic(err)
}
