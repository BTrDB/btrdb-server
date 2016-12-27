package grpcinterface

import (
	"fmt"
	"log"
	"net"
	"strings"

	"golang.org/x/net/context"

	"net/http"
	_ "net/http/pprof"

	"github.com/SoftwareDefinedBuildings/btrdb"
	"github.com/SoftwareDefinedBuildings/btrdb/bte"
	"github.com/SoftwareDefinedBuildings/btrdb/qtree"
	"github.com/SoftwareDefinedBuildings/btrdb/version"
	"google.golang.org/grpc"
)

//go:generate protoc -I/usr/local/include -I. -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --go_out=Mgoogle/api/annotations.proto=github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api,plugins=grpc:. btrdb.proto
//go:generate protoc -I/usr/local/include -I. -I$GOPATH/src -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis  --grpc-gateway_out=logtostderr=true:.  btrdb.proto

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

type apiProvider struct {
	b *btrdb.Quasar
}

func ServeGRPC(q *btrdb.Quasar, laddr string) {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	l, err := net.Listen("tcp", laddr)
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()

	RegisterBTrDBServer(grpcServer, &apiProvider{q})
	grpcServer.Serve(l)
}

type TimeParam interface {
	Start() int64
	End() int64
}

func validTimes(s, e int64) bool {
	if s >= e ||
		s < MinimumTime ||
		e >= MaximumTime {
		return false
	}
	return true
}

func validValues(v []*RawPoint) bool {
	return true
}

//TODO check contract:
// functions must not close error channel
// functions must not close value channel if there was an error
// functions must not write to error channel if they are blocking on sending to value channel (avoid leak)
// functions must treat a context cancel as an error and obey the above rules
func (a *apiProvider) RawValues(p *RawValuesParams, r BTrDB_RawValuesServer) error {
	ctx := r.Context()
	if !validTimes(p.Start, p.End) {
		return r.Send(&RawValuesResponse{Stat: ErrBadTimes})
	}
	ver := p.VersionMajor
	if ver == 0 {
		ver = btrdb.LatestGeneration
	}
	recordc, errorc, gen := a.b.QueryValuesStream(ctx, p.Uuid, p.Start, p.End, ver)
	rw := make([]*RawPoint, RawBatchSize)
	cnt := 0
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
				if cnt > 0 {
					return r.Send(&RawValuesResponse{
						Values:       rw[:cnt],
						VersionMajor: gen,
					})
				}
				return nil
			}
			rw[cnt] = &RawPoint{Time: pnt.Time, Value: pnt.Val}
			cnt++
			if cnt >= RawBatchSize {
				err := r.Send(&RawValuesResponse{
					Values:       rw[:cnt],
					VersionMajor: gen,
				})
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
	if !validTimes(p.Start, p.End) {
		return r.Send(&AlignedWindowsResponse{Stat: ErrBadTimes})
	}
	ver := p.VersionMajor
	if ver == 0 {
		ver = btrdb.LatestGeneration
	}
	if p.PointWidth > 64 {
		return r.Send(&AlignedWindowsResponse{Stat: ErrBadPW})
	}
	recordc, errorc, gen := a.b.QueryStatisticalValuesStream(ctx, p.Uuid, p.Start, p.End, ver, uint8(p.PointWidth))
	rw := make([]*StatPoint, StatBatchSize)
	cnt := 0
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
				if cnt > 0 {
					return r.Send(&AlignedWindowsResponse{
						Values:       rw[:cnt],
						VersionMajor: gen,
					})
				}
				return nil
			}
			rw[cnt] = &StatPoint{Time: pnt.Time, Min: pnt.Min, Mean: pnt.Mean, Max: pnt.Max, Count: pnt.Count}
			cnt++
			if cnt >= StatBatchSize {
				err := r.Send(&AlignedWindowsResponse{
					Values:       rw[:cnt],
					VersionMajor: gen,
				})
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
	if !validTimes(p.Start, p.End) {
		return r.Send(&WindowsResponse{Stat: ErrBadTimes})
	}
	ver := p.VersionMajor
	if ver == 0 {
		ver = btrdb.LatestGeneration
	}
	//TODO check normal width is okay
	if p.Depth > 64 {
		return r.Send(&WindowsResponse{Stat: ErrBadPW})
	}
	recordc, errorc, gen := a.b.QueryWindow(ctx, p.Uuid, p.Start, p.End, ver, p.Width, uint8(p.Depth))
	rw := make([]*StatPoint, StatBatchSize)
	cnt := 0
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
				if cnt > 0 {
					return r.Send(&WindowsResponse{
						Values:       rw[:cnt],
						VersionMajor: gen,
					})
				}
				return nil
			}
			rw[cnt] = &StatPoint{Time: pnt.Time, Min: pnt.Min, Mean: pnt.Mean, Max: pnt.Max, Count: pnt.Count}
			cnt++
			if cnt >= StatBatchSize {
				err := r.Send(&WindowsResponse{
					Values:       rw[:cnt],
					VersionMajor: gen,
				})
				if err != nil {
					return err
				}
				cnt = 0
			}
		}
	}
}
func (a *apiProvider) StreamInfo(ctx context.Context, p *StreamInfoParams) (*StreamInfoResponse, error) {
	info, ver := a.b.StorageProvider().GetStreamInfo(p.GetUuid())
	if ver == 0 {
		return &StreamInfoResponse{Stat: &Status{
			Code: uint32(bte.NoSuchStream),
			Msg:  "Stream not found",
		}}, nil
	}
	rv := StreamInfoResponse{Uuid: info.UUID(), VersionMajor: ver, VersionMinor: 0, Collection: info.Collection()}
	for k, v := range info.Tags() {
		rv.Tags = append(rv.Tags, &Tag{Key: k, Value: v})
	}
	return &rv, nil
}
func (a *apiProvider) Nearest(ctx context.Context, p *NearestParams) (*NearestResponse, error) {
	ver := p.VersionMajor
	if ver == 0 {
		ver = btrdb.LatestGeneration
	}
	rec, err, gen := a.b.QueryNearestValue(ctx, p.Uuid, p.Time, p.Backward, ver)
	if err != nil {
		return &NearestResponse{Stat: &Status{Code: uint32(err.Code()), Msg: err.Reason()}}, nil
	}
	return &NearestResponse{VersionMajor: gen, VersionMinor: 0, Value: &RawPoint{Time: rec.Time, Value: rec.Val}}, nil
}
func (a *apiProvider) Changes(p *ChangesParams, r BTrDB_ChangesServer) error {
	start := p.FromMajor
	end := p.ToMajor
	if end == 0 {
		end = btrdb.LatestGeneration
	}
	if p.Resolution > 64 {
		return r.Send(&ChangesResponse{Stat: &Status{Code: bte.InvalidPointWidth, Msg: "Invalid resolution parameter"}})
	}
	cval, cerr, gen := a.b.QueryChangedRanges(r.Context(), p.Uuid, start, end, uint8(p.Resolution))
	rw := make([]*ChangedRange, ChangedRangeBatchSize)
	cnt := 0
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
				if cnt > 0 {
					return r.Send(&ChangesResponse{
						Ranges:       rw[:cnt],
						VersionMajor: gen,
					})
				}
				return nil
			}
			rw[cnt] = &ChangedRange{Start: cr.Start, End: cr.End}
			cnt++
			if cnt >= ChangedRangeBatchSize {
				err := r.Send(&ChangesResponse{
					Ranges:       rw[:cnt],
					VersionMajor: gen,
				})
				if err != nil {
					return err
				}
				cnt = 0
			}
		}
	}
}
func (a *apiProvider) Create(ctx context.Context, p *CreateParams) (*CreateResponse, error) {
	tgs := make(map[string]string)
	for _, t := range p.Tags {
		tgs[string(t.Key)] = string(t.Value)
	}
	err := a.b.StorageProvider().CreateStream(p.Uuid, p.Collection, tgs)
	if err != nil {
		bt := bte.MaybeWrap(err)
		return &CreateResponse{Stat: &Status{
			Code: uint32(bt.Code()),
			Msg:  bt.Reason(),
		}}, nil
	}
	return &CreateResponse{}, nil
}
func (a *apiProvider) ListCollections(ctx context.Context, p *ListCollectionsParams) (*ListCollectionsResponse, error) {
	rv, err := a.b.StorageProvider().ListCollections(p.Prefix, p.StartWith, int64(p.Number))
	if err != nil {
		bt := bte.MaybeWrap(err)
		return &ListCollectionsResponse{Stat: &Status{
			Code: uint32(bt.Code()),
			Msg:  bt.Reason(),
		}}, nil
	}
	return &ListCollectionsResponse{Collections: rv}, nil
}
func (a *apiProvider) Insert(ctx context.Context, p *InsertParams) (*InsertResponse, error) {
	fmt.Printf("got insert\n")
	if len(p.Values) > MaxInsertSize {
		return &InsertResponse{Stat: ErrInsertTooBig}, nil
	}
	qtr := make([]qtree.Record, len(p.Values))
	for idx, pv := range p.Values {
		qtr[idx].Time = pv.Time
		qtr[idx].Val = pv.Value
	}
	err := a.b.InsertValues(p.Uuid, qtr)
	fmt.Printf("RESPONDING %v", err)
	if err != nil {
		return &InsertResponse{Stat: &Status{
			Code: uint32(err.Code()),
			Msg:  err.Error(),
		}}, nil
	}
	return &InsertResponse{}, nil
}
func (a *apiProvider) Delete(ctx context.Context, p *DeleteParams) (*DeleteResponse, error) {
	err := a.b.DeleteRange(p.Uuid, p.Start, p.End)
	if err != nil {
		return &DeleteResponse{Stat: &Status{
			Code: uint32(err.Code()),
			Msg:  err.Error(),
		}}, nil
	}
	return &DeleteResponse{}, nil
}
func (a *apiProvider) ListStreams(ctx context.Context, p *ListStreamsParams) (*ListStreamsResponse, error) {
	tgs := make(map[string]string)
	for _, t := range p.Tags {
		tgs[string(t.Key)] = string(t.Value)
	}

	strms, err := a.b.StorageProvider().ListStreams(string(p.Collection), p.Partial, tgs)
	if err != nil {
		return &ListStreamsResponse{Stat: &Status{
			Code: uint32(err.Code()),
			Msg:  err.Reason(),
		}}, nil
	}
	rv := &ListStreamsResponse{Collection: p.Collection}
	for _, s := range strms {
		tgl := []*Tag{}
		for k, v := range s.Tags() {
			tgl = append(tgl, &Tag{Key: k, Value: v})
		}
		rv.StreamListings = append(rv.StreamListings, &StreamListing{
			Uuid: s.UUID(),
			Tags: tgl,
		})
	}
	return rv, nil
}
func (a *apiProvider) Info(context.Context, *InfoParams) (*InfoResponse, error) {
	ccfg := a.b.GetClusterConfiguration()
	cs := ccfg.GetCachedClusterState()
	m := Mash{
		Revision:       cs.Revision,
		Leader:         cs.Leader,
		LeaderRevision: cs.LeaderRevision,
		Healthy:        cs.Healthy(),
		Unmapped:       cs.GapPercentage(),
	}
	cm := cs.CurrentMash()
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

	rv := InfoResponse{Mash: &m, MajorVersion: version.Major, MinorVersion: version.Minor, Build: version.FullVersion()}
	return &rv, nil
}
