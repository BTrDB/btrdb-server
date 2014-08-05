package quasar

import (
	"net/http"
	"strconv"
	"github.com/bmizerany/pat"
	"cal-sdb.org/quasar/qtree"
	"strings"
)

func doError(w http.ResponseWriter, e string) {
	w.WriteHeader(400)
	w.Write([]byte(e))
}
func request_get_VRANGE(q *Quasar, w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	start_time, err := strconv.ParseInt(r.Form.Get("st"), 10, 64)
	if err != nil {
		doError(w, "Invalid start time parameter")
	}
	end_time, err := strconv.ParseInt(r.Form.Get("et"), 10, 64)
	if err != nil {
		doError(w, "Invalid end time parameter")
	}
	if end_time <= start_time {
		doError(w, "Invalid time range")
	}
	encoding := r.Form.Get("enc")
	if encoding == "" {
		encoding = "json"
	} else if encoding != "json" {
		doError(w, "Unsupported output encoding")
	}
	pw, err := strconv.ParseInt(r.Form.Get("pw"), 10, 8)
	if err != nil || pw < 0 || pw > qtree.ROOTPW+qtree.KFACTOR {
		doError(w, "Invalid pointwidth")
	}
	fieldsstr := r.Form.Get("f")
	if fieldsstr == "" {
		fieldsstr = "count,min,mean,max"
	}
	fslice := strings.Split(fieldsstr,",")
	fields := make(map[string]bool)
	for _, f := range fslice {
		fields[f] = true
	}
	
	//Ok do the request
	//results := q
}

func curry(q *Quasar, 
	f func(*Quasar, http.ResponseWriter, *http.Request)) func (w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		f(q,w,r)
	}
}
func QuasarServeHTTP(q *Quasar, addr string) {
	mux := pat.New()
  	mux.Get("/q/:uuid/v", http.HandlerFunc(curry(q, request_get_VRANGE)))
  	//mux.Post("/q/:uuid/v", curry(q, p
	http.ListenAndServe(addr, mux)
}

