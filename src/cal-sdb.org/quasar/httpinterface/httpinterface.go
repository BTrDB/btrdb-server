package httpinterface

import (
	"net/http"
	"strconv"
	"github.com/bmizerany/pat"
	"cal-sdb.org/quasar"
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"log"
)

func doError(w http.ResponseWriter, e string) {
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
	st, ok, msg := parseInt(r.Form.Get("starttime"), -(16<<56), (48<<56))
	if !ok {
		doError(w, "bad start time: "+msg)
		return
	}
	et, ok, msg := parseInt(r.Form.Get("endtime"), -(16<<56), (48<<56))
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
	versioni, ok, msg := parseInt(versions, 0, 1<<63 - 1)
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
	} {unitoftime}
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
	if st >= quasar.MaximumTime / divisor ||
	   st <= quasar.MinimumTime / divisor {
		doError(w, "start time out of bounds")
		return	   	
    }
    if et >= quasar.MaximumTime / divisor ||
	   et <= quasar.MinimumTime / divisor {
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
	log.Printf("pw %v",pw)
	if pws != "" {
		//res, gen, err := q.QueryStatisticalValues(id, st, et, version, pw)
		doError(w, "aint done this yet")
		return
	} else {
		res, rgen, err := q.QueryValues(id, st, et, version)
		resf := make([][]interface{}, len(res))
		contents := make([]interface{},len(res)*2)
		for i:=0; i < len(res); i++ {
			resf[i] = contents[i*2:(i+1)*2]
			resf[i][0] = res[i].Time / divisor
			resf[i][1] = res[i].Val
		}
		if err != nil {
			doError(w, "query error: "+err.Error())
		}
		//props := struct{Uot string `json:"UnitofTime"`}{"foo"}
		rv := []struct{
			Uuid 	 string 	  `json:"uuid"`
			Readings [][]interface{}
			Version  uint64		  `json:"version"`
			Properties interface{} `json:"Properties"`
		} {
			{id.String(), resf, rgen, uot},
		}
		bb, err := json.Marshal(rv)
		if err != nil {
			doError(w, "JSON error: "+err.Error())
			return
		}
		w.Write(bb)
		return
	}
	//res, err := q.
}

func curry(q *quasar.Quasar, 
	f func(*quasar.Quasar, http.ResponseWriter, *http.Request)) func (w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		f(q,w,r)
	}
}
func QuasarServeHTTP(q *quasar.Quasar, addr string) {
	mux := pat.New()
  	mux.Get("/data/uuid/:uuid", http.HandlerFunc(curry(q, request_get_VRANGE)))
  	//mux.Get("/q/versions", http.HandlerFunc(curry(q, request_get_VERSIONS)))
  	//mux.Get("/q/nearest", http.Handler(curry(q, request_get_NEAREST)))
  	//mux.Post("/data/add/:subkey", http.HandlerFunc(curry(q, request_post_INSERT)))
  	//mux.Post("/q/:uuid/v", curry(q, p
  	log.Printf("serving http on %v", addr)
	err := http.ListenAndServe(addr, mux)
	log.Panic(err)
}

