
GOROOT:=$(CURDIR)/build/go
GOPATH:=$(CURDIR)/build/gopath
PATH:=$(PATH):$(GOROOT)/bin:$(GOPATH)/bin
PKGROOT:=$(GOPATH)/src/github.com/BTrDB/btrdb-server
BUILD:=$(CURDIR)/build

main: $(PKGROOT)/btrdbd/btrdbd

.PHONY: main

$(PKGROOT)/btrdbd/btrdbd: $(PKGROOT)/vendor
	cd $(PKGROOT)/btrdbd; go build -o $(BUILD)/btrdbd

$(GOROOT)/bin/go:
	cd build; wget https://dl.google.com/go/go1.10.linux-amd64.tar.gz; tar -xf go1.10.linux-amd64.tar.gz

$(GOPATH)/bin/dep: $(GOROOT)/bin/go
	mkdir -p $(GOPATH)/bin
	go get -u github.com/golang/dep/cmd/dep

$(PKGROOT)/vendor: $(PKGROOT) $(GOROOT)/bin/go $(GOPATH)/bin/dep
	cd $(GOPATH)/src/github.com/BTrDB/btrdb-server &&  dep ensure

$(PKGROOT):
	mkdir -p build/gopath/src/github.com/BTrDB
	ln -s $(CURDIR) build/gopath/src/github.com/BTrDB/btrdb-server
