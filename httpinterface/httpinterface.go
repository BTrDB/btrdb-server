package httpinterface

import (
	"context"
	"io"
	"mime"
	"net/http"
	"strings"

	gw "github.com/SoftwareDefinedBuildings/btrdb/grpcinterface"
	assetfs "github.com/elazarl/go-bindata-assetfs"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
)

//go:generate protoc -I/usr/local/include -I. -I$GOPATH/src -I../grpcinterface/ -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis  --swagger_out=logtostderr=true:. ../grpcinterface/btrdb.proto
//go:generate ./genswag.py
//go:generate go-bindata -pkg httpinterface swagger-ui/...
func serveSwagger(mux *http.ServeMux) {
	mime.AddExtensionType(".svg", "image/svg+xml")

	// Expose files in third_party/swagger-ui/ on <host>/swagger-ui
	fileServer := http.FileServer(&assetfs.AssetFS{
		Asset:     Asset,
		AssetDir:  AssetDir,
		AssetInfo: AssetInfo,
		Prefix:    "swagger-ui",
	})
	prefix := "/swag/"
	mux.Handle(prefix, http.StripPrefix(prefix, fileServer))
}

func Run() error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v4.0/swagger.json", func(w http.ResponseWriter, req *http.Request) {
		io.Copy(w, strings.NewReader(SwaggerJSON))
	})

	gwmux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}
	err := gw.RegisterBTrDBHandlerFromEndpoint(ctx, gwmux, "127.0.0.1:4410", opts)
	if err != nil {
		return err
	}
	mux.Handle("/", gwmux)
	serveSwagger(mux)
	http.ListenAndServe(":9000", mux)
	return nil
}
