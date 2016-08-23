package grpcinterface

import (
	"net"

	"golang.org/x/net/context"

	"github.com/SoftwareDefinedBuildings/btrdb"
	"google.golang.org/grpc"
)

var NotImplemented = &Status{
	Code: 500,
	Msg:  "Not implemented",
}

//go:generate protoc btrdb.proto --go_out=plugins=grpc:.
type apiProvider struct {
	b *btrdb.Quasar
}

func ServeGPRC(q *btrdb.Quasar, laddr string) {
	l, err := net.Listen("tcp", laddr)
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()

	RegisterBTrDBServer(grpcServer, &apiProvider{q})
	grpcServer.Serve(l)
}

func (a *apiProvider) RawValues(p *RawValuesParams, r BTrDB_RawValuesServer) error {
	return r.Send(&RawValuesResponse{Stat: NotImplemented})
}
func (a *apiProvider) AlignedWindows(p *AlignedWindowsParams, r BTrDB_AlignedWindowsServer) error {
	return r.Send(&AlignedWindowsResponse{Stat: NotImplemented})
}
func (a *apiProvider) Windows(p *WindowsParams, r BTrDB_WindowsServer) error {
	return r.Send(&WindowsResponse{Stat: NotImplemented})
}
func (a *apiProvider) Version(ctx context.Context, p *VersionParams) (*VersionResponse, error) {
	return &VersionResponse{Stat: NotImplemented}, nil
}
func (a *apiProvider) Nearest(ctx context.Context, p *NearestParams) (*NearestResponse, error) {
	return &NearestResponse{Stat: NotImplemented}, nil
}
func (a *apiProvider) Changes(ctx context.Context, p *ChangesParams) (*ChangesResponse, error) {
	return &ChangesResponse{Stat: NotImplemented}, nil
}
func (a *apiProvider) Insert(ctx context.Context, p *InsertParams) (*InsertResponse, error) {
	return &InsertResponse{Stat: NotImplemented}, nil
}
func (a *apiProvider) Delete(ctx context.Context, p *DeleteParams) (*DeleteResponse, error) {
	return &DeleteResponse{Stat: NotImplemented}, nil
}
func (a *apiProvider) Info(context.Context, *InfoParams) (*InfoResponse, error) {
	return &InfoResponse{Stat: NotImplemented}, nil
}
