// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

// +build ignore

package bprovider_test

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/BTrDB/btrdb-server/internal/bprovider"
	"github.com/BTrDB/btrdb-server/internal/cephprovider"
	"github.com/op/go-logging"
)

var log *logging.Logger

func init() {
	log = logging.MustGetLogger("log")
}

func makeCephProvider() *cephprovider.CephStorageProvider {
	params := map[string]string{}
	cp := new(cephprovider.CephStorageProvider)
	/*err := cp.CreateDatabase(params)
	if err != nil {
		log.Panicf("Error on create %v",err)
	}*/
	cp.Initialize(params, nil)
	return cp
}

func TestCephInitDB(t *testing.T) {
	params := map[string]string{}
	cp := new(cephprovider.CephStorageProvider)
	err := cp.CreateDatabase(params)
	if err != nil {
		log.Panicf("Error on create %v", err)
	}
}

func x_RW1(t *testing.T, sp bprovider.StorageProvider) {
	seg := sp.LockSegment()
	addr := seg.BaseAddress()
	data := make([]byte, 1024)
	for i := 0; i < 1024; i++ {
		data[i] = byte(i)
	}
	_, err := seg.Write(addr, data)
	if err != nil {
		t.Fatalf("Got error on write: %v", err)
	}
	seg.Unlock()

	//Read back
	rdata := make([]byte, 30000)
	rslice := sp.Read(addr, rdata)
	if len(rslice) != len(data) {
		t.Fatalf("Got wrong slice len back")
	}
	for i := 0; i < 1024; i++ {
		if rslice[i] != data[i] {
			t.Fatalf("Index %v differed got %v, expected %v", i, rslice[i], data[i])
		}
	}
}

func x_RWFuzz(t *testing.T, sp bprovider.StorageProvider) {
	wg := sync.WaitGroup{}
	const par = 2096
	const seglimlim = 50
	const arrszlim = 20482
	const maxseeds = 1
	for si := 1; si <= maxseeds; si++ {
		log.Warning("Trying seed %v", si)
		rand.Seed(int64(si))
		wg.Add(par)
		for li := 0; li < par; li++ {
			lic := li
			go func() {

				seg := sp.LockSegment()
				addr := seg.BaseAddress()
				log.Warning("Segment %v base addr 0x%016x", lic, addr)
				seglimit := 1 //rand.Int() % seglimlim
				stored_data := make([][]byte, seglimit)
				stored_addrs := make([]uint64, seglimit)
				for k := 0; k < seglimit; k++ {
					arrsize := rand.Int() % arrszlim
					data := make([]byte, arrsize)
					for i := 0; i < arrsize; i++ {
						data[i] = byte(rand.Int())
					}
					stored_data[k] = data
					naddr, err := seg.Write(addr, data)
					if err != nil {
						log.Error("ea %v", err)
						t.Errorf("Got error on write: %v", err)
						return
					}
					stored_addrs[k] = addr
					addr = naddr
				}
				seg.Unlock()
				sleeptime := time.Duration(rand.Int() % 2000)
				time.Sleep(sleeptime * time.Millisecond)
				//Read back
				for k := 0; k < seglimit; k++ {
					rdata := make([]byte, 33000)
					rslice := sp.Read(stored_addrs[k], rdata)
					if len(rslice) != len(stored_data[k]) {
						log.Error("eb")
						t.Errorf("Got wrong slice len back")
						return
					}
					for j := 0; j < len(stored_data[k]); j++ {
						if rslice[j] != stored_data[k][j] {
							log.Error("ec")
							t.Errorf("Index %v differed got %v, expected %v", j, rslice[j], stored_data[k][j])
						}
					}
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func Test_FP_RW1(t *testing.T) {
	fp := makeFileProvider()
	x_RW1(t, fp)
}

func Test_FP_FUZZ(t *testing.T) {
	fp := makeFileProvider()
	x_RWFuzz(t, fp)
}

func Test_CP_RW1(t *testing.T) {
	cp := makeCephProvider()
	x_RW1(t, cp)
}

func Test_CP_FUZZ(t *testing.T) {
	cp := makeCephProvider()
	x_RWFuzz(t, cp)
}
