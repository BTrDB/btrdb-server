package grpcinterface

import (
	"fmt"
	"net"

	"golang.org/x/net/context"

	"github.com/SoftwareDefinedBuildings/btrdb"
	"github.com/SoftwareDefinedBuildings/btrdb/qtree"
	"google.golang.org/grpc"
)

//go:generate protoc -I/usr/local/include -I. -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --go_out=Mgoogle/api/annotations.proto=github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api,plugins=grpc:. btrdb.proto
//go:generate protoc -I/usr/local/include -I. -I$GOPATH/src -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis  --grpc-gateway_out=logtostderr=true:.  btrdb.proto

var ErrNotImplemented = &Status{
	Code: 500,
	Msg:  "Not implemented",
}
var ErrBadTimes = &Status{
	Code: 401,
	Msg:  "Invalid time range",
}
var ErrInsertTooBig = &Status{
	Code: 402,
	Msg:  "Insert too big",
}

const MinimumTime = -(16 << 56)
const MaximumTime = (48 << 56)
const MaxInsertSize = 25000
const RawBatchSize = 5000

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
// functions must not write to error channel if they are blocking on sending to value channel
// functions must treat a context cancel as an error and obey the above rules
func (a *apiProvider) RawValues(p *RawValuesParams, r BTrDB_RawValuesServer) error {
	ctx := r.Context()
	if !validTimes(p.Start, p.End) {
		fmt.Println("sending error")
		return r.Send(&RawValuesResponse{Stat: ErrBadTimes})
	}
	fmt.Println("doing query")
	ver := p.VersionMajor
	if ver == 0 {
		fmt.Println("setting to latest gen")
		ver = btrdb.LatestGeneration
	}
	recordc, errorc, gen := a.b.QueryValuesStream(ctx, p.Uuid, p.Start, p.End, ver)
	fmt.Println("read gen was", gen)
	rw := make([]*RawPoint, RawBatchSize)
	cnt := 0
	for {
		select {
		case err := <-errorc:
			fmt.Println("got error reading")
			return r.Send(&RawValuesResponse{
				Stat: &Status{
					Code: uint32(err.Code()),
					Msg:  err.Error(),
				},
			})
		case pnt, ok := <-recordc:
			if !ok {
				fmt.Println("got nok")
				if cnt > 0 {
					r.Send(&RawValuesResponse{
						Values:       rw[:cnt],
						VersionMajor: gen,
					})
				}
				return nil
			}
			fmt.Println("got ok")
			rw[cnt] = &RawPoint{Time: pnt.Time, Value: pnt.Val}
			cnt++
			if cnt >= RawBatchSize {
				r.Send(&RawValuesResponse{
					Values:       rw[:cnt],
					VersionMajor: gen,
				})
				cnt = 0
			}
		}
	}
}
func (a *apiProvider) AlignedWindows(p *AlignedWindowsParams, r BTrDB_AlignedWindowsServer) error {
	return r.Send(&AlignedWindowsResponse{Stat: ErrNotImplemented})
}
func (a *apiProvider) Windows(p *WindowsParams, r BTrDB_WindowsServer) error {
	return r.Send(&WindowsResponse{Stat: ErrNotImplemented})
}
func (a *apiProvider) Version(ctx context.Context, p *VersionParams) (*VersionResponse, error) {
	return &VersionResponse{Stat: ErrNotImplemented}, nil
}
func (a *apiProvider) Nearest(ctx context.Context, p *NearestParams) (*NearestResponse, error) {
	return &NearestResponse{Stat: ErrNotImplemented}, nil
}
func (a *apiProvider) Changes(ctx context.Context, p *ChangesParams) (*ChangesResponse, error) {
	return &ChangesResponse{Stat: ErrNotImplemented}, nil
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
func (a *apiProvider) Info(context.Context, *InfoParams) (*InfoResponse, error) {
	return &InfoResponse{Stat: ErrNotImplemented}, nil
}
