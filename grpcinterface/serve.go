package grpcinterface

import (
	"net"
	"strings"

	"golang.org/x/net/context"

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

type apiProvider struct {
	b *btrdb.Quasar
}

func ServeGRPC(q *btrdb.Quasar, laddr string) {
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
					Msg:  err.Error(),
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
					Msg:  err.Error(),
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
	return &StreamInfoResponse{Stat: ErrNotImplemented}, nil
}
func (a *apiProvider) Nearest(ctx context.Context, p *NearestParams) (*NearestResponse, error) {
	return &NearestResponse{Stat: ErrNotImplemented}, nil
}
func (a *apiProvider) Changes(ctx context.Context, p *ChangesParams) (*ChangesResponse, error) {
	return &ChangesResponse{Stat: ErrNotImplemented}, nil
}
func (a *apiProvider) Create(ctx context.Context, p *CreateParams) (*CreateResponse, error) {
	return &CreateResponse{Stat: ErrNotImplemented}, nil
}
func (a *apiProvider) ListCollections(ctx context.Context, p *ListCollectionsParams) (*ListCollectionsResponse, error) {
	return &ListCollectionsResponse{Stat: ErrNotImplemented}, nil
}
func (a *apiProvider) Insert(ctx context.Context, p *InsertParams) (*InsertResponse, error) {
	if len(p.Values) > MaxInsertSize {
		return &InsertResponse{Stat: ErrInsertTooBig}, nil
	}
	qtr := make([]qtree.Record, len(p.Values))
	for idx, pv := range p.Values {
		qtr[idx].Time = pv.Time
		qtr[idx].Val = pv.Value
	}
	a.b.InsertValues(p.Uuid, qtr)
	/*	if err != nil {
		return &InsertResponse{Stat: &Status{
			Code: err.Code(),
			Msg:  err.Error(),
		}}, nil
	} */
	return &InsertResponse{}, nil
}
func (a *apiProvider) Delete(ctx context.Context, p *DeleteParams) (*DeleteResponse, error) {
	return &DeleteResponse{Stat: ErrNotImplemented}, nil
}
func (a *apiProvider) ListStreams(ctx context.Context, p *ListStreamsParams) (*ListStreamsResponse, error) {
	return &ListStreamsResponse{Stat: ErrNotImplemented}, nil
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
