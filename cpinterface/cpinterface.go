package cpinterface

import (
	"net"
	"os"
	"os/signal"
	"sync"

	"github.com/pborman/uuid"
	"github.com/SoftwareDefinedBuildings/btrdb"
	"github.com/SoftwareDefinedBuildings/btrdb/qtree"
	capn "github.com/glycerine/go-capnproto"
	"github.com/op/go-logging"
)

var log *logging.Logger

func init() {
	log = logging.MustGetLogger("log")
}

type CPInterface struct {
	isShuttingDown bool
}

func ServeCPNP(q *btrdb.Quasar, ntype string, laddr string) *CPInterface {
	rv := &CPInterface{}
	go func() {
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, os.Interrupt)
		_ = <-sigchan
		rv.isShuttingDown = true
	}()
	l, err := net.Listen(ntype, laddr)
	if err != nil {
		log.Panic(err)
	}
	defer l.Close()
	for !rv.isShuttingDown {
		conn, err := l.Accept()
		if err != nil {
			log.Panic(err)
		}
		go func(c net.Conn) {
			rv.dispatchCommands(q, c)
		}(conn)
	}
	return rv
}

func (c *CPInterface) Shutdown() {
	c.isShuttingDown = true
}

func (c *CPInterface) dispatchCommands(q *btrdb.Quasar, conn net.Conn) {
	//This governs the stream
	rmtx := sync.Mutex{}
	wmtx := sync.Mutex{}
	log.Info("cpnp connection")
	for !c.isShuttingDown {
		rmtx.Lock()
		seg, err := capn.ReadFromStream(conn, nil)
		if err != nil {
			log.Warningf("ERR (%v) :: %v", conn.RemoteAddr(), err)
			conn.Close()
			break
		}
		rmtx.Unlock()
		go func() {
			seg := seg
			req := ReadRootRequest(seg)
			mkresp := func() (Response, *capn.Segment) {
				rvseg := capn.NewBuffer(nil)
				resp := NewRootResponse(rvseg)
				resp.SetEchoTag(req.EchoTag())
				return resp, rvseg
			}
			sendresp := func(seg *capn.Segment) {
				wmtx.Lock()
				seg.WriteTo(conn)
				wmtx.Unlock()
			}
			switch req.Which() {
			case REQUEST_QUERYSTANDARDVALUES:
				//log.Info("QSV\n")
				st := req.QueryStandardValues().StartTime()
				et := req.QueryStandardValues().EndTime()
				uuid := uuid.UUID(req.QueryStandardValues().Uuid())
				ver := req.QueryStandardValues().Version()
				//log.Infof("[REQ=QsV] st=%v, et=%v, uuid=%v, gen=%v", st, et, uuid, ver)
				if ver == 0 {
					ver = btrdb.LatestGeneration
				}
				recordc, errorc, gen := q.QueryValuesStream(uuid, st, et, ver)
				if recordc == nil {
					log.Warningf("RESPONDING ERR: %v", err)
					resp, rvseg := mkresp()
					resp.SetStatusCode(STATUSCODE_INTERNALERROR)
					resp.SetFinal(true)
					sendresp(rvseg)
					return
				} else {
					bufarr := make([]qtree.Record, 0, 4096)
					for {
						resp, rvseg := mkresp()
						fail := false
						fin := false
						for {
							select {
							case _, ok := <-errorc:
								if ok {
									fin = true
									fail = true
									goto donestandard
								}
							case r, ok := <-recordc:
								if !ok {
									fin = true
									goto donestandard
								}
								bufarr = append(bufarr, r)
								if len(bufarr) == cap(bufarr) {
									goto donestandard
								}
							}
						}
					donestandard:
						if fail {
							resp.SetStatusCode(STATUSCODE_INTERNALERROR)
							resp.SetFinal(true)
							//consume channels
							go func() {
								for _ = range recordc {
								}
							}()
							go func() {
								for _ = range errorc {
								}
							}()
							sendresp(rvseg)
							return
						}
						records := NewRecords(rvseg)
						rl := NewRecordList(rvseg, len(bufarr))
						rla := rl.ToArray()
						for i, v := range bufarr {
							rla[i].SetTime(v.Time)
							rla[i].SetValue(v.Val)
						}
						records.SetVersion(gen)
						records.SetValues(rl)
						resp.SetRecords(records)
						resp.SetStatusCode(STATUSCODE_OK)
						if fin {
							resp.SetFinal(true)
						}
						sendresp(rvseg)
						bufarr = bufarr[:0]
						if fin {
							return
						}
					}
				}
			case REQUEST_QUERYWINDOWVALUES:
				st := req.QueryWindowValues().StartTime()
				et := req.QueryWindowValues().EndTime()
				id := uuid.UUID(req.QueryWindowValues().Uuid())
				width := req.QueryWindowValues().Width()
				ver := req.QueryWindowValues().Version()
				depth := req.QueryWindowValues().Depth()
				if ver == 0 {
					ver = btrdb.LatestGeneration
				}
				recordc, gen := q.QueryWindow(id, st, et, ver, width, depth)
				if recordc == nil {
					log.Warningf("RESPONDING ERR: %v", err)
					resp, rvseg := mkresp()
					resp.SetStatusCode(STATUSCODE_INTERNALERROR)
					resp.SetFinal(true)
					sendresp(rvseg)
					return
				} else {
					bufarr := make([]qtree.StatRecord, 0, 4096)
					for {
						resp, rvseg := mkresp()
						fail := false
						fin := false
						for {
							select {
							case r, ok := <-recordc:
								if !ok {
									fin = true
									goto donewindow
								}
								bufarr = append(bufarr, r)
								if len(bufarr) == cap(bufarr) {
									goto donewindow
								}
							}
						}
					donewindow:
						if fail {
							resp.SetStatusCode(STATUSCODE_INTERNALERROR)
							resp.SetFinal(true)
							//consume channels
							go func() {
								for _ = range recordc {
								}
							}()
							sendresp(rvseg)
							return
						}
						records := NewStatisticalRecords(rvseg)
						rl := NewStatisticalRecordList(rvseg, len(bufarr))
						rla := rl.ToArray()
						for i, v := range bufarr {
							rla[i].SetTime(v.Time)
							rla[i].SetCount(v.Count)
							rla[i].SetMin(v.Min)
							rla[i].SetMean(v.Mean)
							rla[i].SetMax(v.Max)
						}
						records.SetVersion(gen)
						records.SetValues(rl)
						resp.SetStatisticalRecords(records)
						resp.SetStatusCode(STATUSCODE_OK)
						if fin {
							resp.SetFinal(true)
						}
						sendresp(rvseg)
						bufarr = bufarr[:0]
						if fin {
							return
						}
					}
				}
			case REQUEST_QUERYSTATISTICALVALUES:
				st := req.QueryStatisticalValues().StartTime()
				et := req.QueryStatisticalValues().EndTime()
				uuid := uuid.UUID(req.QueryStatisticalValues().Uuid())
				pw := req.QueryStatisticalValues().PointWidth()
				ver := req.QueryStatisticalValues().Version()
				if ver == 0 {
					ver = btrdb.LatestGeneration
				}
				recordc, errorc, gen := q.QueryStatisticalValuesStream(uuid, st, et, ver, pw)
				if recordc == nil {
					log.Warningf("RESPONDING ERR: %v", err)
					resp, rvseg := mkresp()
					resp.SetStatusCode(STATUSCODE_INTERNALERROR)
					resp.SetFinal(true)
					sendresp(rvseg)
					return
				} else {
					bufarr := make([]qtree.StatRecord, 0, 4096)
					for {
						resp, rvseg := mkresp()
						fail := false
						fin := false
						for {
							select {
							case _, ok := <-errorc:
								if ok {
									fin = true
									fail = true
									goto donestat
								}
							case r, ok := <-recordc:
								if !ok {
									fin = true
									goto donestat
								}
								bufarr = append(bufarr, r)
								if len(bufarr) == cap(bufarr) {
									goto donestat
								}
							}
						}
					donestat:
						if fail {
							resp.SetStatusCode(STATUSCODE_INTERNALERROR)
							resp.SetFinal(true)
							//consume channels
							go func() {
								for _ = range recordc {
								}
							}()
							go func() {
								for _ = range errorc {
								}
							}()
							sendresp(rvseg)
							return
						}
						records := NewStatisticalRecords(rvseg)
						rl := NewStatisticalRecordList(rvseg, len(bufarr))
						rla := rl.ToArray()
						for i, v := range bufarr {
							rla[i].SetTime(v.Time)
							rla[i].SetCount(v.Count)
							rla[i].SetMin(v.Min)
							rla[i].SetMean(v.Mean)
							rla[i].SetMax(v.Max)
						}
						records.SetVersion(gen)
						records.SetValues(rl)
						resp.SetStatisticalRecords(records)
						resp.SetStatusCode(STATUSCODE_OK)
						if fin {
							resp.SetFinal(true)
						}
						sendresp(rvseg)
						bufarr = bufarr[:0]
						if fin {
							return
						}
					}
				}
			case REQUEST_QUERYVERSION:
				//ul := req.
				ul := req.QueryVersion().Uuids()
				ull := ul.ToArray()
				resp, rvseg := mkresp()
				rvers := NewVersions(rvseg)
				vlist := rvseg.NewUInt64List(len(ull))
				ulist := rvseg.NewDataList(len(ull))
				for i, v := range ull {
					ver, err := q.QueryGeneration(uuid.UUID(v))
					if err != nil {
						resp.SetStatusCode(STATUSCODE_INTERNALERROR)
						resp.SetFinal(true)
						sendresp(rvseg)
						return
					}
					//I'm not sure that the array that sits behind the uuid slice will stick around
					//so I'm copying it.
					uuid := make([]byte, 16)
					copy(uuid, v)
					vlist.Set(i, ver)
					ulist.Set(i, uuid)
				}
				resp.SetStatusCode(STATUSCODE_OK)
				rvers.SetUuids(ulist)
				rvers.SetVersions(vlist)
				resp.SetVersionList(rvers)
				resp.SetFinal(true)
				sendresp(rvseg)
			case REQUEST_QUERYNEARESTVALUE:
				resp, rvseg := mkresp()
				t := req.QueryNearestValue().Time()
				id := uuid.UUID(req.QueryNearestValue().Uuid())
				ver := req.QueryNearestValue().Version()
				if ver == 0 {
					ver = btrdb.LatestGeneration
				}
				back := req.QueryNearestValue().Backward()
				rv, gen, err := q.QueryNearestValue(id, t, back, ver)
				switch err {
				case nil:
					resp.SetStatusCode(STATUSCODE_OK)
					records := NewRecords(rvseg)
					rl := NewRecordList(rvseg, 1)
					rla := rl.ToArray()
					rla[0].SetTime(rv.Time)
					rla[0].SetValue(rv.Val)
					records.SetVersion(gen)
					records.SetValues(rl)
					resp.SetRecords(records)
				case qtree.ErrNoSuchPoint:
					resp.SetStatusCode(STATUSCODE_NOSUCHPOINT)
				default:
					resp.SetStatusCode(STATUSCODE_INTERNALERROR)
				}
				resp.SetFinal(true)
				sendresp(rvseg)
			case REQUEST_QUERYCHANGEDRANGES:
				resp, rvseg := mkresp()
				id := uuid.UUID(req.QueryChangedRanges().Uuid())
				sgen := req.QueryChangedRanges().FromGeneration()
				egen := req.QueryChangedRanges().ToGeneration()
				if egen == 0 {
					egen = btrdb.LatestGeneration
				}
				resolution := req.QueryChangedRanges().Resolution()
				rv, ver, err := q.QueryChangedRanges(id, sgen, egen, resolution)
				switch err {
				case nil:
					resp.SetStatusCode(STATUSCODE_OK)
					ranges := NewRanges(rvseg)
					ranges.SetVersion(ver)
					crl := NewChangedRangeList(rvseg, len(rv))
					crla := crl.ToArray()
					for i := 0; i < len(rv); i++ {
						crla[i].SetStartTime(rv[i].Start)
						crla[i].SetEndTime(rv[i].End)
					}
					ranges.SetValues(crl)
					resp.SetChangedRngList(ranges)
				default:
					log.Critical("qcr error: ", err)
					resp.SetStatusCode(STATUSCODE_INTERNALERROR)
				}
				resp.SetFinal(true)
				sendresp(rvseg)

			case REQUEST_INSERTVALUES:
				resp, rvseg := mkresp()
				uuid := uuid.UUID(req.InsertValues().Uuid())
				rl := req.InsertValues().Values()
				rla := rl.ToArray()
				if len(rla) != 0 {
					qtr := make([]qtree.Record, len(rla))
					for i, v := range rla {
						qtr[i] = qtree.Record{Time: v.Time(), Val: v.Value()}
					}
					q.InsertValues(uuid, qtr)
				}
				if req.InsertValues().Sync() {
					q.Flush(uuid)
				}
				resp.SetFinal(true)
				resp.SetStatusCode(STATUSCODE_OK)
				sendresp(rvseg)
			case REQUEST_DELETEVALUES:
				resp, rvseg := mkresp()
				id := uuid.UUID(req.DeleteValues().Uuid())
				stime := req.DeleteValues().StartTime()
				etime := req.DeleteValues().EndTime()
				err := q.DeleteRange(id, stime, etime)
				switch err {
				case nil:
					resp.SetStatusCode(STATUSCODE_OK)
				default:
					resp.SetStatusCode(STATUSCODE_INTERNALERROR)
				}
				resp.SetFinal(true)
				sendresp(rvseg)
			default:
				log.Critical("weird segment")
			}
		}()
	}
}

/*
func EncodeMsg() *bytes.Buffer {
	rv := bytes.Buffer{}
	seg := capn.NewBuffer(nil)
	cmd := NewRootRequest(seg)

	qsv := NewCmdQueryStandardValues(seg)
	cmd.SetEchoTag(500)
	qsv.SetStartTime(0x5a5a)
	qsv.SetEndTime(0xf7f7)
	cmd.SetQueryStandardValues(qsv)
	seg.WriteTo(&rv)
	return &rv
}

func DecodeMsg(b *bytes.Buffer) {
	seg, err := capn.ReadFromStream(b, nil)
	if err != nil {
		log.Panic(err)
	}
	cmd := ReadRootRequest(seg)
	switch cmd.Which() {
	case REQUEST_QUERYSTANDARDVALUES:
		ca := cmd.QueryStandardValues()
	default:
		log.Critical("wtf")
	}
}
*/
