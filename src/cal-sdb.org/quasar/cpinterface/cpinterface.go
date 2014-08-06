package cpinterface

import (
	"cal-sdb.org/quasar"
	"cal-sdb.org/quasar/qtree"
	capn "github.com/glycerine/go-capnproto"
	"log"
	"net"
	"sync"
	"code.google.com/p/go-uuid/uuid"
)

func ServeCPNP(q *quasar.Quasar, ntype string, laddr string) {
	l, err := net.Listen(ntype, laddr)
	if err != nil {
		log.Panic(err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Panic(err)
		}
		go func(c net.Conn) {
			dispatchCommands(q, c)
		}(conn)
	}
}

func dispatchCommands(q *quasar.Quasar, conn net.Conn) {
	//This governs the stream
	mtx := sync.Mutex{}
	for {
		mtx.Lock()
		seg, err := capn.ReadFromStream(conn, nil)
		if err != nil {
			log.Printf("ERR (%v) :: %v", conn.RemoteAddr(), err)
			conn.Close()
			break
		}
		mtx.Unlock()
		go func() {
			req := NewRootRequest(seg)
			rvseg := capn.NewBuffer(nil)
			resp := NewRootResponse(rvseg)
			resp.SetEchoTag(req.EchoTag())
			switch req.Which() {
			case REQUEST_QUERYSTANDARDVALUES:
				st := req.QueryStandardValues().StartTime()
				et := req.QueryStandardValues().EndTime()
				uuid := uuid.UUID(req.QueryStandardValues().Uuid())
				ver := req.QueryStandardValues().Version()
				if ver == 0 {
					ver = quasar.LatestGeneration
				}
				rv, gen, err := q.QueryValues(uuid, st, et, ver)
				switch err {
				case nil:
					resp.SetStatusCode(STATUSCODE_OK)
					records := NewRecords(rvseg)
					rl := NewRecordList(rvseg, len(rv))
					rla := rl.ToArray()
					for i, v := range rv {
						rla[i].SetTime(v.Time)
						rla[i].SetValue(v.Val)
					}
					records.SetVersion(gen)
					records.SetValues(rl)
					resp.SetRecords(records)
				default:
					resp.SetStatusCode(STATUSCODE_INTERNALERROR)
					//TODO specialize this
				}
			case REQUEST_QUERYSTATISTICALVALUES:
				st := req.QueryStatisticalValues().StartTime()
				et := req.QueryStatisticalValues().EndTime()
				uuid := uuid.UUID(req.QueryStatisticalValues().Uuid())
				pw := req.QueryStatisticalValues().PointWidth()
				ver := req.QueryStatisticalValues().Version()
				if ver == 0 {
					ver = quasar.LatestGeneration
				}
				rv, gen, err := q.QueryStatisticalValues(uuid, st, et, ver, pw)
				switch err {
				case nil:
					resp.SetStatusCode(STATUSCODE_OK)
					srecords := NewStatisticalRecords(rvseg)
					rl := NewStatisticalRecordList(rvseg, len(rv))
					rla := rl.ToArray()
					for i, v := range rv {
						rla[i].SetTime(v.Time)
						rla[i].SetCount(v.Count)
						rla[i].SetMin(v.Min)
						rla[i].SetMean(v.Mean)
						rla[i].SetMax(v.Max)
					}
					srecords.SetVersion(gen)
					srecords.SetValues(rl)
					resp.SetStatisticalRecords(srecords)
				default:
					resp.SetStatusCode(STATUSCODE_INTERNALERROR)
				}
				resp.SetStatusCode(STATUSCODE_INTERNALERROR)
			case REQUEST_QUERYVERSION:
				//ul := req.
				ul := req.QueryVersion().Uuids()
				ull := ul.ToArray()
				rvers := NewVersions(rvseg)
				vlist := rvseg.NewUInt64List(len(ull))
				ulist := rvseg.NewDataList(len(ull))
				for i, v := range ull {
					ver, err := q.QueryGeneration(uuid.UUID(v))
					if err != nil {
						resp.SetStatusCode(STATUSCODE_INTERNALERROR)
						break
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
			case REQUEST_QUERYNEARESTVALUE:
				t := req.QueryNearestValue().Time()
				id := uuid.UUID(req.QueryNearestValue().Uuid())
				ver := req.QueryNearestValue().Version()
				if ver == 0 {
					ver = quasar.LatestGeneration
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
					//TODO specialize this
				}
			case REQUEST_QUERYCHANGEDRANGES:
				resp.SetStatusCode(STATUSCODE_INTERNALERROR)
			case REQUEST_INSERTVALUES:
				uuid := uuid.UUID(req.InsertValues().Uuid())
				rl := req.InsertValues().Values()
				rla := rl.ToArray()
				qtr := make([]qtree.Record, len(rla))
				for i, v := range rla {
					qtr[i] = qtree.Record{Time: v.Time(), Val: v.Value()}
				}
				q.InsertValues(uuid, qtr)
				//TODO add support for the sync variable
				resp.SetStatusCode(STATUSCODE_OK)
			case REQUEST_DELETEVALUES:
				resp.SetStatusCode(STATUSCODE_INTERNALERROR)
			}
			mtx.Lock()
			rvseg.WriteTo(conn)
			mtx.Unlock()
		}()
	}
}

/*
func EncodeMsg() *bytes.Buffer {
	rv := bytes.Buffer{}
	seg := capn.NewBuffer(nil)
	cmd := NewRootRequest(seg)
	ca := NewCmdA(seg)
	ca.SetCmd(50)
	cmd.SetCa(ca)
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
	case REQUEST_CA:
		ca := cmd.Ca()
		log.Printf("ca val: %v", ca.Cmd())
	default:
		log.Printf("wtf")
	}
}
*/
