// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

package cephprovider

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

func (sp *CephStorageProvider) BackgroundCleanup(uuids [][]byte) error {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		sp.bgClean(false, uuids)
		wg.Done()
	}()
	if sp.hotPool != sp.dataPool {
		wg.Add(1)
		sp.bgClean(true, uuids)
		wg.Done()
	}
	wg.Wait()
	return nil
}

func (sp *CephStorageProvider) bgClean(isHot bool, uuids [][]byte) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	freed := uint64(0)
	scanned := uint64(0)
	go func() {
		poolname := sp.dataPool
		if isHot {
			poolname = sp.hotPool
		}
		for {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(30 * time.Second)
			lg.Infof("[%s] BG CLEANER %d objects freed, %dk objects scanned", poolname, freed, scanned/1000)
		}
	}()
	oidprefixes := []string{}
	for _, uu := range uuids {
		oidprefix := fmt.Sprintf("%032x", uu)
		oidprefixes = append(oidprefixes, oidprefix)
		sbprefix := fmt.Sprintf("sb%032x", uu)
		oidprefixes = append(oidprefixes, sbprefix)
	}
	rmh, h, err := sp.getHandle(context.Background(), isHot)
	if err != nil {
		panic(err)
	}
	rmh2, h2, err := sp.getHandle(context.Background(), isHot)
	if err != nil {
		panic(err)
	}
	defer rmh.Release()
	defer rmh2.Release()
	lfunc := func(oid string) {
		mustDelete := false
		for _, pfx := range oidprefixes {
			if strings.HasPrefix(oid, pfx) {
				mustDelete = true
				break
			}
		}
		if mustDelete {
			err := h2.Delete(oid)
			if err != nil {
				lg.Panicf("could not free object in BG scan: %v", err)
			}
			freed++
		}
		scanned++
	}
	err = h.ListObjects(lfunc)
	if err != nil {
		lg.Panicf("could not list objects to do delete: %v\n", err)
	}
}
