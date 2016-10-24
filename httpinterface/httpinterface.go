package httpinterface

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/SoftwareDefinedBuildings/btrdb"
	"github.com/SoftwareDefinedBuildings/btrdb/qtree"
	"github.com/bmizerany/pat"
	"github.com/op/go-logging"
	"github.com/pborman/uuid"
	"github.com/stretchr/graceful"
)

var outstandingHttpReqs int32 = 0
var log *logging.Logger

func init() {
	log = logging.MustGetLogger("log")
}

func doError(w http.ResponseWriter, e string) {
	log.Warning("returning error %v", e)
	w.WriteHeader(400)
	w.Write([]byte(e))
}

func logh(t string, more string, r *http.Request) {
	ff := r.Header.Get("X-FORWARDED-FOR")
	log.Info("HRQ %s %s %s <<%s>>", t, r.RemoteAddr, ff, more)
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
func request_get_VRANGE(q *btrdb.Quasar, w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&outstandingHttpReqs, 1)
	defer func() {
		atomic.AddInt32(&outstandingHttpReqs, -1)
	}()
	r.ParseForm()
	ids := r.Form.Get(":uuid")
	id := uuid.Parse(ids)
	if id == nil {
		log.Critical("ids: '%v'", ids)
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
		version = btrdb.LatestGeneration
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
	if st >= btrdb.MaximumTime/divisor ||
		st <= btrdb.MinimumTime/divisor {
		doError(w, "start time out of bounds")
		return
	}
	if et >= btrdb.MaximumTime/divisor ||
		et <= btrdb.MinimumTime/divisor {
		doError(w, "end time out of bounds")
		return
	}
	st *= divisor
	et *= divisor
	wins := r.Form.Get("win")
	pws := r.Form.Get("pw")
	if wins != "" && pws != "" {
		doError(w, "cannot have pointwidth and window")
		return
	} else if wins != "" {
		winl, ok, msg := parseInt(wins, 1, math.MaxInt64)
		if !ok {
			doError(w, "bad window width: "+msg)
			return
		}
		if divisor != 1 {
			doError(w, "statistical results require unitoftime=ns")
			return
		}
		logh("QWV", fmt.Sprintf("st=%v et=%v win=%v u=%s", st, et, uint64(winl), id.String()), r)
		//TODO add depth
		res, rgen := q.QueryWindow(id, st, et, version, uint64(winl), 0)
		if res == nil {
			doError(w, "query error")
			return
		}
		cachedRes := make([]qtree.StatRecord, 0)
		for r := range res {
			cachedRes = append(cachedRes, r)
		}
		resf := make([][]interface{}, len(cachedRes))
		contents := make([]interface{}, len(cachedRes)*6)
		i := 0
		for _, r := range cachedRes {
			resf[i] = contents[i*6 : (i+1)*6]
			resf[i][0] = r.Time / 1000000 //ms since epoch
			resf[i][1] = r.Time % 1000000 //nanoseconds left over
			resf[i][2] = r.Min
			resf[i][3] = r.Mean
			resf[i][4] = r.Max
			resf[i][5] = r.Count
			i++
		}
		rv := []struct {
			Uuid      string `json:"uuid"`
			XReadings [][]interface{}
			Version   uint64 `json:"version"`
		}{
			{id.String(), resf, rgen},
		}
		err := json.NewEncoder(w).Encode(rv)
		if err != nil {
			doError(w, "JSON error: "+err.Error())
			return
		}
		return
	} else if pws != "" {
		pw := uint8(0)
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
		//log.Info("HTTP REQ id=%s pw=%v", id.String(), pw)
		logh("QSV", fmt.Sprintf("st=%v et=%v pw=%v u=%s", st, et, pw, id.String()), r)
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
		logh("QV", fmt.Sprintf("st=%v et=%v u=%s", st, et, id.String()), r)
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
}

type insert_t struct {
	Uuid     string `json:"uuid"`
	Readings [][]interface{}
}

func request_post_INSERT(q *btrdb.Quasar, w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&outstandingHttpReqs, 1)
	defer func() {
		atomic.AddInt32(&outstandingHttpReqs, -1)
	}()
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
		t, ok, msg := parseInt(string(ins.Readings[i][0].(json.Number)), btrdb.MinimumTime, btrdb.MaximumTime)
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

func request_post_LEGACYINSERT(q *btrdb.Quasar, w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&outstandingHttpReqs, 1)
	defer func() {
		atomic.AddInt32(&outstandingHttpReqs, -1)
	}()
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
			t, ok, msg := parseInt(string(r.Readings[i][0].(json.Number)), btrdb.MinimumTime, btrdb.MaximumTime)
			if !ok {
				doError(w, fmt.Sprintf("reading %d time malformed: %s", i, msg))
				return
			}

			if t >= (btrdb.MaximumTime/uotmult) || t <= (btrdb.MinimumTime/uotmult) {
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
func curry(q *btrdb.Quasar,
	f func(*btrdb.Quasar, http.ResponseWriter, *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		f(q, w, r)
	}
}
func request_get_NEAREST(q *btrdb.Quasar, w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&outstandingHttpReqs, 1)
	defer func() {
		atomic.AddInt32(&outstandingHttpReqs, -1)
	}()
	r.ParseForm()
	ids := r.Form.Get(":uuid")
	id := uuid.Parse(ids)
	if id == nil {
		doError(w, "malformed uuid")
		return
	}
	t, ok, msg := parseInt(r.Form.Get("time"), -(16 << 56), (48 << 56))
	if !ok {
		doError(w, "bad time: "+msg)
		return
	}
	bws := r.Form.Get("backwards")
	bw := bws != ""
	rec, _, err := q.QueryNearestValue(id, t, bw, btrdb.LatestGeneration)
	if err != nil {
		doError(w, "Bad query: "+err.Error())
		return
	}
	w.Write([]byte(fmt.Sprintf("[%d, %f]", rec.Time, rec.Val)))
}

type bracket_req struct {
	UUIDS []string
}
type bracket_resp struct {
	Brackets [][]int64
	Merged   []int64
}

func request_post_BRACKET(q *btrdb.Quasar, w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&outstandingHttpReqs, 1)
	defer func() {
		atomic.AddInt32(&outstandingHttpReqs, -1)
	}()
	dec := json.NewDecoder(r.Body)
	req := bracket_req{}
	err := dec.Decode(&req)
	if err != nil {
		doError(w, "bad request")
		return
	}
	if len(req.UUIDS) == 0 {
		doError(w, "no uuids")
		return
	}
	rv := bracket_resp{}
	rv.Brackets = make([][]int64, len(req.UUIDS))
	var min, max int64
	var minset, maxset bool
	for i, u := range req.UUIDS {
		uid := uuid.Parse(u)
		if uid == nil {
			doError(w, "malformed uuid")
			return
		}
		rec, _, err := q.QueryNearestValue(uid, btrdb.MinimumTime+1, false, btrdb.LatestGeneration)
		if err == qtree.ErrNoSuchStream {
			rv.Brackets[i] = make([]int64, 2)
			rv.Brackets[i][0] = -1
			rv.Brackets[i][1] = -1
			continue
		}
		if err != nil {
			doError(w, "Bad query: "+err.Error())
			return
		}
		start := rec.Time
		if !minset || start < min {
			min = start
			minset = true
		}
		rec, _, err = q.QueryNearestValue(uid, btrdb.MaximumTime-1, true, btrdb.LatestGeneration)
		if err != nil {
			doError(w, "Bad query: "+err.Error())
			return
		}
		end := rec.Time
		if !maxset || end > max {
			max = end
			maxset = true
		}
		rv.Brackets[i] = make([]int64, 2)
		rv.Brackets[i][0] = start
		rv.Brackets[i][1] = end
	}
	rv.Merged = make([]int64, 2)
	if minset && maxset {
		rv.Merged[0] = min
		rv.Merged[1] = max
	} else {
		doError(w, "Bad query: none of those streams exist")
		return
	}

	err = json.NewEncoder(w).Encode(rv)
	if err != nil {
		doError(w, "JSON error: "+err.Error())
		return
	}
	return
}

func request_get_STATUS(q *btrdb.Quasar, w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}
func QuasarServeHTTP(q *btrdb.Quasar, addr string) {
	go func() {
		log.Info("Active HTTP requests: ", outstandingHttpReqs)
	}()
	mux := pat.New()
	mux.Get("/data/uuid/:uuid", http.HandlerFunc(curry(q, request_get_VRANGE)))
	mux.Get("/csv/uuid/:uuid", http.HandlerFunc(curry(q, request_get_CSV)))
	mux.Post("/directcsv", http.HandlerFunc(curry(q, request_post_MULTICSV)))
	mux.Post("/wrappedcsv", http.HandlerFunc(curry(q, request_post_WRAPPED_MULTICSV)))
	//mux.Get("/q/versions", http.HandlerFunc(curry(q, request_get_VERSIONS)))
	mux.Get("/q/nearest/:uuid", http.HandlerFunc(curry(q, request_get_NEAREST)))
	mux.Post("/q/bracket", http.HandlerFunc(curry(q, request_post_BRACKET)))
	mux.Post("/data/add/:subkey", http.HandlerFunc(curry(q, request_post_INSERT)))
	mux.Post("/data/legacyadd/:subkey", http.HandlerFunc(curry(q, request_post_LEGACYINSERT)))
	mux.Get("/status", http.HandlerFunc(curry(q, request_get_STATUS)))
	//mux.Post("/q/:uuid/v", curry(q, p
	log.Info("serving http on %v", addr)
	graceful.Run(addr, 10*time.Second, mux)
}
