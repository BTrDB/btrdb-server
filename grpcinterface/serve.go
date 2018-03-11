package grpcinterface

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"time"

	"context"

	"net/http"
	_ "net/http/pprof"

	"github.com/BTrDB/btrdb-server"
	"github.com/BTrDB/btrdb-server/bte"
	"github.com/BTrDB/btrdb-server/internal/rez"
	"github.com/BTrDB/btrdb-server/qtree"
	"github.com/BTrDB/btrdb-server/version"
	logging "github.com/op/go-logging"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

var logger *logging.Logger

func init() {
	logger = logging.MustGetLogger("log")
}

//go:generate protoc -I/usr/local/include -I. -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --go_out=Mgoogle/api/annotations.proto=github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api,plugins=grpc:. btrdb.proto
// // # go:generate protoc -I/usr/local/include -I. -I$GOPATH/src -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis  --grpc-gateway_out=logtostderr=true:.  btrdb.proto

var ErrNotImplemented = &Status{
	Code: bte.NotImplemented,
	Msg:  "Not implemented",
}
var ErrBadTimes = &Status{
	Code: bte.InvalidTimeRange,
	Msg:  "Invalid time range",
}
var ErrInsertTooBig = &Status{
	Code: bte.InsertTooBig,
	Msg:  "Insert too big",
}
var ErrBadPW = &Status{
	Code: bte.InvalidPointWidth,
	Msg:  "Bad point width",
}

const MinimumTime = -(16 << 56)
const MaximumTime = (48 << 56)
const MaxInsertSize = 25000
const RawBatchSize = 5000
const StatBatchSize = 5000

const ChangedRangeBatchSize = 1000
const LookupStreamsBatchSize = 200

type apiProvider struct {
	b   *btrdb.Quasar
	s   *grpc.Server
	rez *rez.RezManager
}

type GRPCInterface interface {
	InitiateShutdown() chan struct{}
}

func ServeGRPC(q *btrdb.Quasar, laddr string) GRPCInterface {
	go func() {
		fmt.Println("==== PROFILING ENABLED ==========")
		runtime.SetBlockProfileRate(5000)
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe("0.0.0.0:6060", nil)
		panic(err)
	}()
	fmt.Printf("Listening on %s\n", laddr)
	l, err := net.Listen("tcp", laddr)
	if err != nil {
		panic(err)
	}
	gzcp := grpc.NewGZIPCompressor()
	grpcServer := grpc.NewServer(grpc.RPCCompressor(gzcp))
	api := &apiProvider{b: q,
		s:   grpcServer,
		rez: q.Rez()}
	RegisterBTrDBServer(grpcServer, api)
	go grpcServer.Serve(l)
	return api
}

type TimeParam interface {
	Start() int64
	End() int64
}

func (a *apiProvider) InitiateShutdown() chan struct{} {
	done := make(chan struct{})
	go func() {
		a.s.GracefulStop()
		close(done)
	}()
	return done
}

//TODO check contract:
// functions must not close error channel
// functions must not close value channel if there was an error
// functions must not write to error channel if they are blocking on sending to value channel (avoid leak)
// functions must treat a context cancel as an error and obey the above rules
func (a *apiProvider) RawValues(p *RawValuesParams, r BTrDB_RawValuesServer) error {
	ctx := r.Context()
	span, ctx := opentracing.StartSpanFromContext(ctx, "RawValues")
	defer span.Finish()
	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
	if err != nil {
		return r.Send(&RawValuesResponse{
			Stat: &Status{
				Code: uint32(err.Code()),
				Msg:  err.Reason(),
			},
		})
	}
	defer res.Release()
	ver := p.VersionMajor
	if ver == 0 {
		ver = btrdb.LatestGeneration
	}
	recordc, errorc, maj, min := a.b.QueryValuesStream(ctx, p.Uuid, p.Start, p.End, ver)
	rw := make([]*RawPoint, RawBatchSize)
	cnt := 0
	havesent := false
	for {
		select {
		case err := <-errorc:
			return r.Send(&RawValuesResponse{
				Stat: &Status{
					Code: uint32(err.Code()),
					Msg:  err.Reason(),
				},
			})
		case pnt, ok := <-recordc:
			if !ok {
				if cnt > 0 || !havesent {
					return r.Send(&RawValuesResponse{
						Values:       rw[:cnt],
						VersionMajor: maj,
						VersionMinor: min,
					})
				}
				return nil
			}
			rw[cnt] = &RawPoint{Time: pnt.Time, Value: pnt.Val}
			cnt++
			if cnt >= RawBatchSize {
				err := r.Send(&RawValuesResponse{
					Values:       rw[:cnt],
					VersionMajor: maj,
					VersionMinor: min,
				})
				havesent = true
				if err != nil {
					return err
				}
				cnt = 0
			}
		}
	}
}
func (a *apiProvider) AlignedWindows(p *AlignedWindowsParams, r BTrDB_AlignedWindowsServer) error {
	ctx := r.Context()
	span, ctx := opentracing.StartSpanFromContext(ctx, "AlignedWindows")
	defer span.Finish()
	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
	if err != nil {
		return r.Send(&AlignedWindowsResponse{
			Stat: &Status{
				Code: uint32(err.Code()),
				Msg:  err.Reason(),
			},
		})
	}
	defer res.Release()
	ver := p.VersionMajor
	if ver == 0 {
		ver = btrdb.LatestGeneration
	}
	if p.PointWidth > 64 {
		return r.Send(&AlignedWindowsResponse{Stat: ErrBadPW})
	}
	recordc, errorc, maj, min := a.b.QueryStatisticalValuesStream(ctx, p.Uuid, p.Start, p.End, ver, uint8(p.PointWidth))
	rw := make([]*StatPoint, StatBatchSize)
	cnt := 0
	havesent := false
	for {
		select {
		case err := <-errorc:
			return r.Send(&AlignedWindowsResponse{
				Stat: &Status{
					Code: uint32(err.Code()),
					Msg:  err.Reason(),
				},
			})
		case pnt, ok := <-recordc:
			if !ok {
				if cnt > 0 || !havesent {
					return r.Send(&AlignedWindowsResponse{
						Values:       rw[:cnt],
						VersionMajor: maj,
						VersionMinor: min,
					})
				}
				return nil
			}
			rw[cnt] = &StatPoint{Time: pnt.Time, Min: pnt.Min, Mean: pnt.Mean, Max: pnt.Max, Count: pnt.Count}
			cnt++
			if cnt >= StatBatchSize {
				err := r.Send(&AlignedWindowsResponse{
					Values:       rw[:cnt],
					VersionMajor: maj,
					VersionMinor: min,
				})
				havesent = true
				if err != nil {
					return err
				}
				cnt = 0
			}
		}
	}
}
func (a *apiProvider) Windows(p *WindowsParams, r BTrDB_WindowsServer) error {
	ctx := r.Context()
	span, ctx := opentracing.StartSpanFromContext(ctx, "Windows")
	defer span.Finish()
	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
	if err != nil {
		return r.Send(&WindowsResponse{
			Stat: &Status{
				Code: uint32(err.Code()),
				Msg:  err.Reason(),
			},
		})
	}
	defer res.Release()
	ver := p.VersionMajor
	if ver == 0 {
		ver = btrdb.LatestGeneration
	}
	recordc, errorc, maj, min := a.b.QueryWindow(ctx, p.Uuid, p.Start, p.End, ver, p.Width, uint8(p.Depth))
	rw := make([]*StatPoint, StatBatchSize)
	cnt := 0
	havesent := false
	for {
		select {
		case err := <-errorc:
			return r.Send(&WindowsResponse{
				Stat: &Status{
					Code: uint32(err.Code()),
					Msg:  err.Error(),
				},
			})
		case pnt, ok := <-recordc:
			if !ok {
				if cnt > 0 || !havesent {
					return r.Send(&WindowsResponse{
						Values:       rw[:cnt],
						VersionMajor: maj,
						VersionMinor: min,
					})
				}
				return nil
			}
			rw[cnt] = &StatPoint{Time: pnt.Time, Min: pnt.Min, Mean: pnt.Mean, Max: pnt.Max, Count: pnt.Count}
			cnt++
			if cnt >= StatBatchSize {
				err := r.Send(&WindowsResponse{
					Values:       rw[:cnt],
					VersionMajor: maj,
					VersionMinor: min,
				})
				havesent = true
				if err != nil {
					return err
				}
				cnt = 0
			}
		}
	}
}
func (a *apiProvider) StreamInfo(ctx context.Context, p *StreamInfoParams) (*StreamInfoResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "StreamInfo")
	defer span.Finish()
	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
	if err != nil {
		return &StreamInfoResponse{
			Stat: &Status{
				Code: uint32(err.Code()),
				Msg:  err.Reason(),
			},
		}, nil
	}
	defer res.Release()
	resp := &StreamInfoResponse{}
	if p.OmitDescriptor && p.OmitVersion {
		return &StreamInfoResponse{
			Stat: &Status{
				Code: uint32(bte.WrongArgs),
				Msg:  "you cannot omit descriptor and version",
			},
		}, nil
	}
	resp.Descriptor_ = &StreamDescriptor{
		Uuid: p.Uuid,
	}
	if !p.OmitDescriptor {
		desc, err := a.b.GetStreamDescriptor(ctx, p.Uuid)
		if err != nil {
			return &StreamInfoResponse{
				Stat: &Status{
					Code: uint32(err.Code()),
					Msg:  err.Reason(),
				},
			}, nil
		}
		resp.Descriptor_.Collection = desc.Collection
		resp.Descriptor_.AnnotationVersion = desc.AnnotationVersion
		for k, v := range desc.Tags {
			resp.Descriptor_.Tags = append(resp.Descriptor_.Tags, &KeyValue{Key: k, Value: []byte(v)})
		}
		for k, v := range desc.Annotations {
			resp.Descriptor_.Annotations = append(resp.Descriptor_.Annotations, &KeyValue{Key: k, Value: []byte(v)})
		}
	}
	if !p.OmitVersion {
		maj, min, err := a.b.GetStreamVersion(ctx, p.Uuid)
		if err != nil {
			return &StreamInfoResponse{
				Stat: &Status{
					Code: uint32(err.Code()),
					Msg:  err.Reason(),
				},
			}, nil
		}
		resp.VersionMajor = maj
		resp.VersionMinor = min
	}
	return resp, nil
}

//
// func (a *apiProvider) StreamAnnotation(ctx context.Context, p *StreamAnnotationParams) (*StreamAnnotationResponse, error) {
// 	ctx, tcancel := context.WithTimeout(ctx, MaxOpTime)
// 	defer tcancel()
// 	span, ctx := opentracing.StartSpanFromContext(ctx, "StreamAnnotation")
// 	defer span.Finish()
// 	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
// 	if err != nil {
// 		return &StreamAnnotationResponse{
// 			Stat: &Status{
// 				Code: uint32(err.Code()),
// 				Msg:  err.Reason(),
// 			},
// 		}, nil
// 	}
// 	defer res.Release()
//
// 	ann, annver, err := a.b.StorageProvider().GetStreamAnnotation(p.Uuid)
// 	if err != nil {
// 		return &StreamAnnotationResponse{Stat: &Status{
// 			Code: uint32(err.Code()),
// 			Msg:  err.Error(),
// 		}}, nil
// 	}
// 	return &StreamAnnotationResponse{AnnotationVersion: annver, Annotation: ann}, nil
// }
//
func (a *apiProvider) SetStreamAnnotations(ctx context.Context, p *SetStreamAnnotationsParams) (*SetStreamAnnotationsResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "SetStreamAnnotation")
	defer span.Finish()
	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
	if err != nil {
		return &SetStreamAnnotationsResponse{
			Stat: &Status{
				Code: uint32(err.Code()),
				Msg:  err.Reason(),
			},
		}, nil
	}
	defer res.Release()

	changes := make(map[string]*string)
	for _, kv := range p.Annotations {
		if kv.Val == nil {
			changes[kv.Key] = nil
		} else {
			s := string(kv.Val.Value)
			changes[kv.Key] = &s
		}
	}
	err = a.b.SetStreamAnnotations(ctx, p.Uuid, p.ExpectedAnnotationVersion, changes)
	if err != nil {
		return &SetStreamAnnotationsResponse{Stat: &Status{
			Code: uint32(err.Code()),
			Msg:  err.Error(),
		}}, nil
	}
	return &SetStreamAnnotationsResponse{}, nil
}

func (a *apiProvider) Create(ctx context.Context, p *CreateParams) (*CreateResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Create")
	defer span.Finish()
	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
	if err != nil {
		return &CreateResponse{
			Stat: &Status{
				Code: uint32(err.Code()),
				Msg:  err.Reason(),
			},
		}, nil
	}
	defer res.Release()

	tgs := make(map[string]string)
	for _, t := range p.Tags {
		tgs[string(t.Key)] = string(t.Value)
	}
	anns := make(map[string]string)
	for _, a := range p.Annotations {
		anns[string(a.Key)] = string(a.Value)
	}
	err = a.b.CreateStream(ctx, p.Uuid, p.Collection, tgs, anns)
	if err != nil {
		return &CreateResponse{Stat: &Status{
			Code: uint32(err.Code()),
			Msg:  err.Reason(),
		}}, nil
	}
	return &CreateResponse{}, nil
}
func (a *apiProvider) ListCollections(ctx context.Context, p *ListCollectionsParams) (*ListCollectionsResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "ListCollections")
	defer span.Finish()
	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
	if err != nil {
		return &ListCollectionsResponse{
			Stat: &Status{
				Code: uint32(err.Code()),
				Msg:  err.Reason(),
			},
		}, nil
	}
	defer res.Release()

	rv, err := a.b.ListCollections(ctx, p.Prefix, p.StartWith, p.Limit)
	if err != nil {
		return &ListCollectionsResponse{Stat: &Status{
			Code: uint32(err.Code()),
			Msg:  err.Reason(),
		}}, nil
	}
	return &ListCollectionsResponse{Collections: rv}, nil
}
func (a *apiProvider) LookupStreams(p *LookupStreamsParams, r BTrDB_LookupStreamsServer) error {
	ctx := r.Context()
	span, ctx := opentracing.StartSpanFromContext(ctx, "LookupStream")
	defer span.Finish()
	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
	if err != nil {
		return r.Send(&LookupStreamsResponse{
			Stat: &Status{
				Code: uint32(err.Code()),
				Msg:  err.Reason(),
			},
		})
	}
	defer res.Release()

	tags := make(map[string]*string)
	for _, kv := range p.Tags {
		if kv.Val == nil {
			tags[kv.Key] = nil
		} else {
			s := string(kv.Val.Value)
			tags[kv.Key] = &s
		}
	}
	anns := make(map[string]*string)
	for _, kv := range p.Annotations {
		if kv.Val == nil {
			anns[kv.Key] = nil
		} else {
			s := string(kv.Val.Value)
			anns[kv.Key] = &s
		}
	}

	cval, cerr := a.b.LookupStreams(ctx, p.Collection, p.IsCollectionPrefix, tags, anns)
	//TODO change this to use append to empty slice. This doesn't help anyone
	rw := []*StreamDescriptor{}
	havesent := false
	for {
		select {
		case err := <-cerr:
			return r.Send(&LookupStreamsResponse{
				Stat: &Status{
					Code: uint32(err.Code()),
					Msg:  err.Error(),
				},
			})
		case cr, ok := <-cval:
			if !ok {
				if len(rw) > 0 || !havesent {
					return r.Send(&LookupStreamsResponse{
						Results: rw,
					})
				}
				return nil
			}
			/*
			   message StreamDescriptor {
			     bytes uuid = 1;
			     string collection = 2;
			     repeated KeyValue tags = 3;
			     repeated KeyValue annotations = 4;
			     uint64 annotationVersion = 5;
			   }
			*/
			des := &StreamDescriptor{Uuid: cr.UUID, Collection: cr.Collection, AnnotationVersion: cr.AnnotationVersion}
			for k, v := range cr.Tags {
				des.Tags = append(des.Tags, &KeyValue{Key: k, Value: []byte(v)})
			}
			for k, v := range cr.Annotations {
				des.Annotations = append(des.Annotations, &KeyValue{Key: k, Value: []byte(v)})
			}
			rw = append(rw, des)
			if len(rw) >= ChangedRangeBatchSize {
				err := r.Send(&LookupStreamsResponse{
					Results: rw,
				})
				havesent = true
				if err != nil {
					return err
				}
				rw = rw[:0]
			}
		}
	}
}
func (a *apiProvider) Nearest(ctx context.Context, p *NearestParams) (*NearestResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Nearest")
	defer span.Finish()
	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
	if err != nil {
		return &NearestResponse{
			Stat: &Status{
				Code: uint32(err.Code()),
				Msg:  err.Reason(),
			},
		}, nil
	}
	defer res.Release()

	ver := p.VersionMajor
	if ver == 0 {
		ver = btrdb.LatestGeneration
	}
	rec, err, maj, min := a.b.QueryNearestValue(ctx, p.Uuid, p.Time, p.Backward, ver)
	if err != nil {
		return &NearestResponse{Stat: &Status{Code: uint32(err.Code()), Msg: err.Reason()}}, nil
	}
	return &NearestResponse{VersionMajor: maj, VersionMinor: min, Value: &RawPoint{Time: rec.Time, Value: rec.Val}}, nil
}
func (a *apiProvider) Changes(p *ChangesParams, r BTrDB_ChangesServer) error {
	ctx := r.Context()
	span, ctx := opentracing.StartSpanFromContext(ctx, "Changes")
	defer span.Finish()
	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
	if err != nil {
		return r.Send(&ChangesResponse{
			Stat: &Status{
				Code: uint32(err.Code()),
				Msg:  err.Reason(),
			},
		})
	}
	defer res.Release()

	start := p.FromMajor
	end := p.ToMajor
	if end == 0 {
		end = btrdb.LatestGeneration
	}
	cval, cerr, maj, min := a.b.QueryChangedRanges(r.Context(), p.Uuid, start, end, uint8(p.Resolution))
	rw := make([]*ChangedRange, ChangedRangeBatchSize)
	cnt := 0
	havesent := false
	for {
		select {
		case err := <-cerr:
			return r.Send(&ChangesResponse{
				Stat: &Status{
					Code: uint32(err.Code()),
					Msg:  err.Error(),
				},
			})
		case cr, ok := <-cval:
			if !ok {
				if cnt > 0 || !havesent {
					return r.Send(&ChangesResponse{
						Ranges:       rw[:cnt],
						VersionMajor: maj,
						VersionMinor: min,
					})
				}
				return nil
			}
			rw[cnt] = &ChangedRange{Start: cr.Start, End: cr.End}
			cnt++
			if cnt >= ChangedRangeBatchSize {
				err := r.Send(&ChangesResponse{
					Ranges:       rw[:cnt],
					VersionMajor: maj,
					VersionMinor: min,
				})
				havesent = true
				if err != nil {
					return err
				}
				cnt = 0
			}
		}
	}
}

func (a *apiProvider) Insert(ctx context.Context, p *InsertParams) (*InsertResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Insert")
	defer span.Finish()
	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
	if err != nil {
		return &InsertResponse{
			Stat: &Status{
				Code: uint32(err.Code()),
				Msg:  err.Reason(),
			},
		}, nil
	}
	defer res.Release()

	if len(p.Values) > MaxInsertSize {
		return &InsertResponse{Stat: ErrInsertTooBig}, nil
	}
	qtr := make([]qtree.Record, len(p.Values))
	for idx, pv := range p.Values {
		qtr[idx].Time = pv.Time
		qtr[idx].Val = pv.Value
	}
	maj, min, err := a.b.InsertValues(ctx, p.Uuid, qtr)
	if err != nil {
		return &InsertResponse{Stat: &Status{
			Code: uint32(err.Code()),
			Msg:  err.Error(),
		}}, nil
	}
	return &InsertResponse{VersionMajor: maj, VersionMinor: min}, nil
}
func (a *apiProvider) Delete(ctx context.Context, p *DeleteParams) (*DeleteResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Insert")
	defer span.Finish()
	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
	if err != nil {
		return &DeleteResponse{
			Stat: &Status{
				Code: uint32(err.Code()),
				Msg:  err.Reason(),
			},
		}, nil
	}
	defer res.Release()

	maj, min, err := a.b.DeleteRange(ctx, p.Uuid, p.Start, p.End)
	if err != nil {
		return &DeleteResponse{Stat: &Status{
			Code: uint32(err.Code()),
			Msg:  err.Error(),
		}}, nil
	}
	return &DeleteResponse{VersionMajor: maj, VersionMinor: min}, nil
}

func (a *apiProvider) Flush(ctx context.Context, p *FlushParams) (*FlushResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Flush")
	defer span.Finish()
	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
	if err != nil {
		return &FlushResponse{
			Stat: &Status{
				Code: uint32(err.Code()),
				Msg:  err.Reason(),
			},
		}, nil
	}
	defer res.Release()

	maj, min, err := a.b.Flush(ctx, p.Uuid)
	if err != nil {
		return &FlushResponse{Stat: &Status{
			Code: uint32(err.Code()),
			Msg:  err.Error(),
		}}, nil
	}
	return &FlushResponse{VersionMajor: maj, VersionMinor: min}, nil
}

func (a *apiProvider) Obliterate(ctx context.Context, p *ObliterateParams) (*ObliterateResponse, error) {
	if os.Getenv("BTRDB_ENABLE_OBLITERATE") != "YES" {
		return &ObliterateResponse{Stat: &Status{
			Code: bte.ObliterateDisabled,
			Msg:  "Obliterate is disabled on this node",
		}}, nil
	}
	span, ctx := opentracing.StartSpanFromContext(ctx, "Obliterate")
	defer span.Finish()
	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
	if err != nil {
		return &ObliterateResponse{
			Stat: &Status{
				Code: uint32(err.Code()),
				Msg:  err.Reason(),
			},
		}, nil
	}
	defer res.Release()

	err = a.b.ObliterateStream(ctx, p.Uuid)
	if err != nil {
		return &ObliterateResponse{Stat: &Status{
			Code: uint32(err.Code()),
			Msg:  err.Error(),
		}}, nil
	}
	return &ObliterateResponse{}, nil
}

func (a *apiProvider) FaultInject(ctx context.Context, fip *FaultInjectParams) (*FaultInjectResponse, error) {
	if os.Getenv("BTRDB_ENABLE_FAULT_INJECT") != "YES" {
		return &FaultInjectResponse{Stat: &Status{
			Code: bte.FaultInjectionDisabled,
			Msg:  "Fault injection is disabled on this node",
		}}, nil
	}
	if fip.Type == 1 {
		panic("Injected panic")
	}
	if fip.Type == 2 {
		go func() {
			time.Sleep(3 * time.Second)
			fmt.Println("DOING INJECTED FAULT")
			time.Sleep(1 * time.Second)
			panic("Delayed injected panic")
		}()
	}
	return &FaultInjectResponse{}, nil
}

func (a *apiProvider) Info(ctx context.Context, params *InfoParams) (*InfoResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Info")
	defer span.Finish()
	res, err := a.rez.Get(ctx, rez.ConcurrentOp)
	if err != nil {
		return &InfoResponse{
			Stat: &Status{
				Code: uint32(err.Code()),
				Msg:  err.Reason(),
			},
		}, nil
	}
	defer res.Release()

	ccfg := a.b.GetClusterConfiguration()
	cs := ccfg.GetCachedClusterState()
	m := Mash{
		Revision:       cs.Revision,
		Leader:         cs.Leader,
		LeaderRevision: cs.LeaderRevision,
		Healthy:        cs.Healthy(),
		Unmapped:       cs.GapPercentage(),
	}
	cm := cs.ProposedMASH()
	m.TotalWeight = cm.TotalWeight
	mmap := make(map[string]*Member)
	for _, member := range cs.Members {
		nm := &Member{
			Hash:           member.Hash,
			Nodename:       member.Nodename,
			Up:             member.Active > 0,
			In:             member.IsIn(),
			Enabled:        member.Enabled,
			Start:          0,
			End:            0,
			Weight:         member.Weight,
			ReadPreference: member.ReadWeight,
			HttpEndpoints:  strings.Join(member.AdvertisedEndpointsHTTP, ";"),
			GrpcEndpoints:  strings.Join(member.AdvertisedEndpointsGRPC, ";"),
		}
		mmap[member.Nodename] = nm
		m.Members = append(m.Members, nm)
	}
	//There may be members not in the mash
	for i := 0; i < len(cm.Nodenames); i++ {
		mp, ok := mmap[cm.Nodenames[i]]
		if ok {
			mp.Start = cm.Ranges[i].Start
			mp.End = cm.Ranges[i].End
		}
	}

	rv := InfoResponse{Mash: &m, MajorVersion: version.Major, MinorVersion: version.Minor, Build: version.VersionString}
	return &rv, nil
}
