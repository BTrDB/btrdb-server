// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

package mprovider

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/BTrDB/btrdb-server/bte"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/pborman/uuid"
)

/*

// Sets the stream annotations. An entry with a nil string implies delete
SetStreamAnnotations(ctx context.Context, uuid []byte, aver uint64, changes map[string]*string) bte.BTE

// Get a stream annotations and tags
GetStreamInfo(ctx context.Context, uuid []byte) (res *LookupResult, err bte.BTE)

// CreateStream makes a stream with the given uuid, collection and tags. Returns
// an error if the uuid already exists.
CreateStream(ctx context.Context, uuid []byte, collection string, tags map[string]string, annotations map[string]string) bte.BTE

// DeleteStream tombstones a stream
DeleteStream(ctx context.Context, uuid []byte) bte.BTE

// ListCollections returns a list of collections beginning with prefix (which may be "")
// and starting from the given string. If number is > 0, only that many results
// will be returned. More can be obtained by re-calling ListCollections with
// a given startingFrom and number.
ListCollections(ctx context.Context, prefix string, startingFrom string, number int64) ([]string, bte.BTE)

// Return back all streams in all collections beginning with collection (or exactly equal if prefix is false)
// provided they have the given tags and annotations, where a nil entry in the map means has the tag but the value is irrelevant
LookupStreams(ctx context.Context, collection string, isCollectionPrefix bool, tags map[string]*string, annotations map[string]*string) (chan *LookupResult, chan bte.BTE)

*/

func helperRandomString(l int) string {
	//Don't judge, i'm over it
	rv := ""
	for len(rv) < l {
		rv += uuid.NewRandom().String()[:4]
	}
	return rv[:l]
}
func helperGetEM(t *testing.T) (context.Context, *etcdMetadataProvider) {
	ec, err := etcd.New(etcd.Config{
		Endpoints:   []string{"http://compound-0.cs.berkeley.edu:2379"},
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Skipf("cannot connect to local etcd: %v", err)
	}
	pfx := fmt.Sprintf("%x", uuid.NewRandom().String()[:8])
	rv := &etcdMetadataProvider{ec: ec, pfx: pfx}
	return context.Background(), rv
}

func TestCreateStreamETagsEAnn(t *testing.T) {
	ctx, em := helperGetEM(t)
	uu := uuid.NewRandom()
	col := fmt.Sprintf("test.%x", uu)

	tags := make(map[string]string)
	anns := make(map[string]string)

	err := em.CreateStream(ctx, uu, col, tags, anns)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lr, err := em.GetStreamInfo(ctx, uu)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(lr.Tags, tags) {
		t.Fatalf("stream tags not equal")
	}
	if !reflect.DeepEqual(lr.Annotations, anns) {
		t.Fatalf("stream annotations not equal")
	}
	if lr.Collection != col {
		t.Fatalf("stream collection not equal")
	}
	if !bytes.Equal(lr.UUID, uu) {
		t.Fatalf("uuid not equal")
	}
}

func TestCreateStreamNilTagsNilAnn(t *testing.T) {
	ctx, em := helperGetEM(t)
	uu := uuid.NewRandom()
	col := fmt.Sprintf("test.%x", uu)

	tags := make(map[string]string)
	anns := make(map[string]string)

	err := em.CreateStream(ctx, uu, col, nil, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lr, err := em.GetStreamInfo(ctx, uu)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(lr.Tags, tags) {
		t.Fatalf("stream tags not equal")
	}
	if !reflect.DeepEqual(lr.Annotations, anns) {
		t.Fatalf("stream annotations not equal")
	}
	if lr.Collection != col {
		t.Fatalf("stream collection not equal")
	}
	if !bytes.Equal(lr.UUID, uu) {
		t.Fatalf("uuid not equal")
	}
}

func TestCreateStreamMinTagsMinAnn(t *testing.T) {
	ctx, em := helperGetEM(t)
	uu := uuid.NewRandom()
	col := fmt.Sprintf("test.%x", uu)

	tags := make(map[string]string)
	tags["hello"] = "foobar"
	anns := make(map[string]string)
	anns["world"] = "bazinga"

	err := em.CreateStream(ctx, uu, col, tags, anns)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lr, err := em.GetStreamInfo(ctx, uu)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(lr.Tags, tags) {
		t.Fatalf("stream tags not equal")
	}
	if !reflect.DeepEqual(lr.Annotations, anns) {
		t.Fatalf("stream annotations not equal")
	}
	if lr.Collection != col {
		t.Fatalf("stream collection not equal")
	}
	if !bytes.Equal(lr.UUID, uu) {
		t.Fatalf("uuid not equal")
	}
}
func TestCreateStreamMaxTagsMaxAnn(t *testing.T) {
	ctx, em := helperGetEM(t)
	uu := uuid.NewRandom()
	col := fmt.Sprintf("test.%x", uu)

	tags := make(map[string]string)
	for i := 0; i < MaximumTags; i++ {
		tags[helperRandomString(MaxTagKeyLength)] = helperRandomString(MaxTagValLength)
	}
	anns := make(map[string]string)
	for i := 0; i < MaximumAnnotations; i++ {
		anns[helperRandomString(MaxAnnKeyLength)] = helperRandomString(MaxAnnValLength)
	}
	err := em.CreateStream(ctx, uu, col, tags, anns)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lr, err := em.GetStreamInfo(ctx, uu)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(lr.Tags, tags) {
		t.Fatalf("stream tags not equal")
	}
	if !reflect.DeepEqual(lr.Annotations, anns) {
		t.Fatalf("stream annotations not equal")
	}
	if lr.Collection != col {
		t.Fatalf("stream collection not equal")
	}
	if !bytes.Equal(lr.UUID, uu) {
		t.Fatalf("uuid not equal")
	}
}

type hellaEntry struct {
	uu   []byte
	tags map[string]string
	anns map[string]string
	col  string
}

func TestCreateHellaStreamMaxTagsMaxAnn(t *testing.T) {
	ctx, em := helperGetEM(t)
	hz := []hellaEntry{}
	fmt.Println("creating")
	icreate := time.Now()
	for k := 0; k < 100000; k++ {
		if k%1000 == 0 {
			fmt.Printf("at %dk\n", k/1000)
		}
		uu := uuid.NewRandom()
		col := fmt.Sprintf("test.%x", uu)

		tags := make(map[string]string)
		for i := 0; i < MaximumTags; i++ {
			tags[helperRandomString(MaxTagKeyLength)] = helperRandomString(MaxTagValLength)
		}
		anns := make(map[string]string)
		for i := 0; i < MaximumAnnotations; i++ {
			anns[helperRandomString(MaxAnnKeyLength)] = helperRandomString(MaxAnnValLength)
		}
		then := time.Now()
		err := em.CreateStream(ctx, uu, col, tags, anns)
		if time.Now().Sub(then) > 3000*time.Millisecond {
			fmt.Printf("create exceeded 3s: %s", time.Now().Sub(then))
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		hz = append(hz, hellaEntry{uu: uu, tags: tags, anns: anns, col: col})
	}
	fmt.Printf("first create done took %s\n", time.Now().Sub(icreate))
	time.Sleep(10 * time.Second)
	for k := 0; k < 12; k++ {
		uu := uuid.NewRandom()
		col := fmt.Sprintf("test.%x", uu)

		tags := make(map[string]string)
		for i := 0; i < MaximumTags; i++ {
			tags[helperRandomString(MaxTagKeyLength)] = helperRandomString(MaxTagValLength)
		}
		anns := make(map[string]string)
		for i := 0; i < MaximumAnnotations; i++ {
			anns[helperRandomString(MaxAnnKeyLength)] = helperRandomString(MaxAnnValLength)
		}
		then := time.Now()
		err := em.CreateStream(ctx, uu, col, tags, anns)
		delta := time.Now().Sub(then)
		if delta > 500*time.Millisecond {
			t.Fatalf("create exceeded 500ms: %s", delta)
		}
		fmt.Printf("delta %d was %s\n", k, delta)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		hz = append(hz, hellaEntry{uu: uu, tags: tags, anns: anns, col: col})
	}
	for _, he := range hz {
		then := time.Now()
		lr, err := em.GetStreamInfo(ctx, he.uu)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if time.Now().Sub(then) > 500*time.Millisecond {
			t.Fatalf("get stream info exceeded 500ms: %s", time.Now().Sub(then))
		}
		if !reflect.DeepEqual(lr.Tags, he.tags) {
			t.Fatalf("stream tags not equal")
		}
		if !reflect.DeepEqual(lr.Annotations, he.anns) {
			t.Fatalf("stream annotations not equal")
		}
		if lr.Collection != he.col {
			t.Fatalf("stream collection not equal")
		}
		if !bytes.Equal(lr.UUID, he.uu) {
			t.Fatalf("uuid not equal")
		}
	}
}

func TestCreate10kStreamMaxTagsMaxAnn(t *testing.T) {
	ctx, em := helperGetEM(t)
	hz := []hellaEntry{}
	fmt.Println("creating")
	icreate := time.Now()
	for k := 0; k < 10000; k++ {
		if k%100 == 0 {
			fmt.Printf("at %dh\n", k/100)
		}
		uu := uuid.NewRandom()
		col := fmt.Sprintf("test.%x", uu)

		tags := make(map[string]string)
		for i := 0; i < MaximumTags; i++ {
			tags[helperRandomString(MaxTagKeyLength)] = helperRandomString(MaxTagValLength)
		}
		anns := make(map[string]string)
		for i := 0; i < MaximumAnnotations; i++ {
			anns[helperRandomString(MaxAnnKeyLength)] = helperRandomString(MaxAnnValLength)
		}
		then := time.Now()
		err := em.CreateStream(ctx, uu, col, tags, anns)
		if time.Now().Sub(then) > 3000*time.Millisecond {
			fmt.Printf("create exceeded 1s: %s", time.Now().Sub(then))
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		hz = append(hz, hellaEntry{uu: uu, tags: tags, anns: anns, col: col})
		//time.Sleep(20 * time.Millisecond)
	}
	fmt.Printf("first create done took %s\n", time.Now().Sub(icreate))
	time.Sleep(10 * time.Second)
	for k := 0; k < 12; k++ {
		uu := uuid.NewRandom()
		col := fmt.Sprintf("test.%x", uu)

		tags := make(map[string]string)
		for i := 0; i < MaximumTags; i++ {
			tags[helperRandomString(MaxTagKeyLength)] = helperRandomString(MaxTagValLength)
		}
		anns := make(map[string]string)
		for i := 0; i < MaximumAnnotations; i++ {
			anns[helperRandomString(MaxAnnKeyLength)] = helperRandomString(MaxAnnValLength)
		}
		then := time.Now()
		err := em.CreateStream(ctx, uu, col, tags, anns)
		delta := time.Now().Sub(then)
		if delta > 500*time.Millisecond {
			t.Fatalf("create exceeded 500ms: %s", delta)
		}
		fmt.Printf("delta %d was %s\n", k, delta)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		hz = append(hz, hellaEntry{uu: uu, tags: tags, anns: anns, col: col})
	}
	for _, he := range hz {
		then := time.Now()
		lr, err := em.GetStreamInfo(ctx, he.uu)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if time.Now().Sub(then) > 500*time.Millisecond {
			t.Fatalf("get stream info exceeded 500ms: %s", time.Now().Sub(then))
		}
		if !reflect.DeepEqual(lr.Tags, he.tags) {
			t.Fatalf("stream tags not equal")
		}
		if !reflect.DeepEqual(lr.Annotations, he.anns) {
			t.Fatalf("stream annotations not equal")
		}
		if lr.Collection != he.col {
			t.Fatalf("stream collection not equal")
		}
		if !bytes.Equal(lr.UUID, he.uu) {
			t.Fatalf("uuid not equal")
		}
	}
}

func TestCreatePHellaStreamMaxTagsMaxAnn(t *testing.T) {
	ctx, em := helperGetEM(t)
	hz := []hellaEntry{}
	fmt.Println("creating")
	icreate := time.Now()
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(100)
	for kk := 0; kk < 100; kk++ {
		go func() {
			for k := 0; k < 100; k++ {
				if k%1000 == 0 {
					fmt.Printf("at %dk\n", k/1000)
				}
				uu := uuid.NewRandom()
				col := fmt.Sprintf("test.%x", uu)

				tags := make(map[string]string)
				for i := 0; i < MaximumTags; i++ {
					tags[helperRandomString(MaxTagKeyLength)] = helperRandomString(MaxTagValLength)
				}
				anns := make(map[string]string)
				for i := 0; i < MaximumAnnotations; i++ {
					anns[helperRandomString(MaxAnnKeyLength)] = helperRandomString(MaxAnnValLength)
				}
				then := time.Now()
				err := em.CreateStream(ctx, uu, col, tags, anns)
				if time.Now().Sub(then) > 3000*time.Millisecond {
					fmt.Printf("create exceeded 3s: %s\n", time.Now().Sub(then))
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				mu.Lock()
				hz = append(hz, hellaEntry{uu: uu, tags: tags, anns: anns, col: col})
				mu.Unlock()
			}
			fmt.Printf("done\n")
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Printf("first create done took %s\n", time.Now().Sub(icreate))
	time.Sleep(10 * time.Second)
	for k := 0; k < 12; k++ {
		uu := uuid.NewRandom()
		col := fmt.Sprintf("test.%x", uu)

		tags := make(map[string]string)
		for i := 0; i < MaximumTags; i++ {
			tags[helperRandomString(MaxTagKeyLength)] = helperRandomString(MaxTagValLength)
		}
		anns := make(map[string]string)
		for i := 0; i < MaximumAnnotations; i++ {
			anns[helperRandomString(MaxAnnKeyLength)] = helperRandomString(MaxAnnValLength)
		}
		then := time.Now()
		err := em.CreateStream(ctx, uu, col, tags, anns)
		delta := time.Now().Sub(then)
		if delta > 500*time.Millisecond {
			t.Fatalf("create exceeded 500ms: %s", delta)
		}
		fmt.Printf("delta %d was %s\n", k, delta)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		mu.Lock()
		hz = append(hz, hellaEntry{uu: uu, tags: tags, anns: anns, col: col})
		mu.Unlock()
	}
	for _, he := range hz {
		then := time.Now()
		lr, err := em.GetStreamInfo(ctx, he.uu)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if time.Now().Sub(then) > 500*time.Millisecond {
			t.Fatalf("get stream info exceeded 500ms: %s", time.Now().Sub(then))
		}
		if !reflect.DeepEqual(lr.Tags, he.tags) {
			t.Fatalf("stream tags not equal")
		}
		if !reflect.DeepEqual(lr.Annotations, he.anns) {
			t.Fatalf("stream annotations not equal")
		}
		if lr.Collection != he.col {
			t.Fatalf("stream collection not equal")
		}
		if !bytes.Equal(lr.UUID, he.uu) {
			t.Fatalf("uuid not equal")
		}
	}
}

func TestCreatePHellaStreamSomeTagsSomeAnn(t *testing.T) {
	ctx, em := helperGetEM(t)
	hz := []hellaEntry{}
	fmt.Println("creating")
	icreate := time.Now()
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(100)
	for kk := 0; kk < 500; kk++ {
		kki := kk
		go func() {
			for k := 0; k < 2000; k++ {
				if k%1000 == 0 {
					fmt.Printf("at %dk (%d)\n", k/1000, kki)
				}
				uu := uuid.NewRandom()
				col := fmt.Sprintf("test.%x", uu)

				tags := make(map[string]string)
				for i := 0; i < 5; i++ {
					tags[helperRandomString(MaxTagKeyLength/2)] = helperRandomString(MaxTagValLength / 2)
				}
				anns := make(map[string]string)
				for i := 0; i < 5; i++ {
					anns[helperRandomString(MaxAnnKeyLength/2)] = helperRandomString(MaxAnnValLength / 2)
				}
				then := time.Now()
				err := em.CreateStream(ctx, uu, col, tags, anns)
				if time.Now().Sub(then) > 3000*time.Millisecond {
					fmt.Printf("create exceeded 3s: %s\n", time.Now().Sub(then))
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				mu.Lock()
				hz = append(hz, hellaEntry{uu: uu, tags: tags, anns: anns, col: col})
				mu.Unlock()
			}
			fmt.Printf("done\n")
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Printf("first create done took %s\n", time.Now().Sub(icreate))
	time.Sleep(10 * time.Second)
	for k := 0; k < 12; k++ {
		uu := uuid.NewRandom()
		col := fmt.Sprintf("test.%x", uu)

		tags := make(map[string]string)
		for i := 0; i < MaximumTags; i++ {
			tags[helperRandomString(MaxTagKeyLength)] = helperRandomString(MaxTagValLength)
		}
		anns := make(map[string]string)
		for i := 0; i < MaximumAnnotations; i++ {
			anns[helperRandomString(MaxAnnKeyLength)] = helperRandomString(MaxAnnValLength)
		}
		then := time.Now()
		err := em.CreateStream(ctx, uu, col, tags, anns)
		delta := time.Now().Sub(then)
		if delta > 500*time.Millisecond {
			t.Fatalf("create exceeded 500ms: %s", delta)
		}
		fmt.Printf("delta %d was %s\n", k, delta)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		mu.Lock()
		hz = append(hz, hellaEntry{uu: uu, tags: tags, anns: anns, col: col})
		mu.Unlock()
	}
	for _, he := range hz {
		then := time.Now()
		lr, err := em.GetStreamInfo(ctx, he.uu)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if time.Now().Sub(then) > 500*time.Millisecond {
			t.Fatalf("get stream info exceeded 500ms: %s", time.Now().Sub(then))
		}
		if !reflect.DeepEqual(lr.Tags, he.tags) {
			t.Fatalf("stream tags not equal")
		}
		if !reflect.DeepEqual(lr.Annotations, he.anns) {
			t.Fatalf("stream annotations not equal")
		}
		if lr.Collection != he.col {
			t.Fatalf("stream collection not equal")
		}
		if !bytes.Equal(lr.UUID, he.uu) {
			t.Fatalf("uuid not equal")
		}
	}
}

func TestCreatePHellaStreamMaxTagsMaxAnnImpl2(t *testing.T) {
	ctx, em := helperGetEM(t)
	hz := []hellaEntry{}
	fmt.Println("creating")
	icreate := time.Now()
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(100)
	for kk := 0; kk < 100; kk++ {
		go func() {
			for k := 0; k < 100; k++ {
				if k%1000 == 0 {
					fmt.Printf("at %dk\n", k/1000)
				}
				uu := uuid.NewRandom()
				col := fmt.Sprintf("test.%x", uu)

				tags := make(map[string]string)
				for i := 0; i < MaximumTags; i++ {
					tags[helperRandomString(MaxTagKeyLength)] = helperRandomString(MaxTagValLength)
				}
				anns := make(map[string]string)
				for i := 0; i < MaximumAnnotations; i++ {
					anns[helperRandomString(MaxAnnKeyLength)] = helperRandomString(MaxAnnValLength)
				}
				then := time.Now()
				err := em.CreateStream2(ctx, uu, col, tags, anns)
				if time.Now().Sub(then) > 3000*time.Millisecond {
					fmt.Printf("create exceeded 3s: %s\n", time.Now().Sub(then))
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				mu.Lock()
				hz = append(hz, hellaEntry{uu: uu, tags: tags, anns: anns, col: col})
				mu.Unlock()
			}
			fmt.Printf("done\n")
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Printf("first create done took %s\n", time.Now().Sub(icreate))
	time.Sleep(10 * time.Second)
	for k := 0; k < 12; k++ {
		uu := uuid.NewRandom()
		col := fmt.Sprintf("test.%x", uu)

		tags := make(map[string]string)
		for i := 0; i < MaximumTags; i++ {
			tags[helperRandomString(MaxTagKeyLength)] = helperRandomString(MaxTagValLength)
		}
		anns := make(map[string]string)
		for i := 0; i < MaximumAnnotations; i++ {
			anns[helperRandomString(MaxAnnKeyLength)] = helperRandomString(MaxAnnValLength)
		}
		then := time.Now()
		err := em.CreateStream(ctx, uu, col, tags, anns)
		delta := time.Now().Sub(then)
		if delta > 500*time.Millisecond {
			t.Fatalf("create exceeded 500ms: %s", delta)
		}
		fmt.Printf("delta %d was %s\n", k, delta)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		mu.Lock()
		hz = append(hz, hellaEntry{uu: uu, tags: tags, anns: anns, col: col})
		mu.Unlock()
	}
	for _, he := range hz {
		then := time.Now()
		lr, err := em.GetStreamInfo(ctx, he.uu)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if time.Now().Sub(then) > 500*time.Millisecond {
			t.Fatalf("get stream info exceeded 500ms: %s", time.Now().Sub(then))
		}
		if !reflect.DeepEqual(lr.Tags, he.tags) {
			t.Fatalf("stream tags not equal")
		}
		if !reflect.DeepEqual(lr.Annotations, he.anns) {
			t.Fatalf("stream annotations not equal")
		}
		if lr.Collection != he.col {
			t.Fatalf("stream collection not equal")
		}
		if !bytes.Equal(lr.UUID, he.uu) {
			t.Fatalf("uuid not equal")
		}
	}
}

func TestDeleteStream(t *testing.T) {
	ctx, em := helperGetEM(t)
	uu := uuid.NewRandom()
	col := fmt.Sprintf("test.%x", uu)
	err := em.CreateStream(ctx, uu, col, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lr, err := em.GetStreamInfo(ctx, uu)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lr == nil {
		t.Fatalf("expected a stream")
	}
	err = em.DeleteStream(ctx, uu)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = em.GetStreamInfo(ctx, uu)
	if err == nil || err.Code() != bte.NoSuchStream {
		t.Fatalf("expected a error: %v", err)
	}
}

// func TestEtcdLimit(t *testing.T) {
//   cl, _ := clientv3.New(clientv3.Config{
// 		Endpoints:   []string{"http://localhost:2379"},
// 		DialTimeout: 3 * time.Second,
// 	})
//   //Insert 100 records
//   uu := uuid.NewRandom().String()
// 	for i := 0; i < 100; i++ {
// 		k := fmt.Sprintf("tst/%s/%04d", uu, i)
// 		_, err := cl.Put(context.Background(), k, "-")
// 		if err != nil {
// 			t.Fatalf("unexpected error: %v", err)
// 		}
// 	}
//   //Query with a limit
//   prefix := fmt.Sprintf("tst/%s/", uu)
//   resp, err := cl.Get(ctx, prefix, etcd.WithLimit(10), etcd.WithPrefix())
//   if err != nil {
//     t.Fatalf("unexpected error: %v", err)
//   }
//   if resp
// }
func TestMore(t *testing.T) {
	ctx, em := helperGetEM(t)
	uu := uuid.NewRandom().String()
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("tst/%s/%04d", uu, i)
		_, err := em.ec.Put(ctx, k, "helloworld")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	pfx := fmt.Sprintf("tst/%s/", uu)
	kv, err := em.ec.Get(ctx, pfx, etcd.WithLimit(10), etcd.WithPrefix())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(kv.Kvs) != 10 {
		t.Fatalf("expected 10 records, got %d", kv.Count)
	}
	if !kv.More {
		t.Fatalf("expected more==true")
	}
	if string(kv.Kvs[9].Key) != pfx+"0009" {
		t.Fatalf("expected different key, got %s", kv.Kvs[9].Key)
	}
	kv, err = em.ec.Get(ctx, pfx+"0050", etcd.WithLimit(10), etcd.WithRange(etcd.GetPrefixRangeEnd(pfx)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(kv.Kvs) != 10 {
		t.Fatalf("expected 10 records, got %d", kv.Count)
	}
	if !kv.More {
		t.Fatalf("expected more==true")
	}
	if string(kv.Kvs[9].Key) != pfx+"0059" {
		t.Fatalf("expected different key, got %s", kv.Kvs[9].Key)
	}
	kv, err = em.ec.Get(ctx, pfx, etcd.WithLimit(100), etcd.WithRange(etcd.GetPrefixRangeEnd(pfx)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(kv.Kvs) != 100 {
		t.Fatalf("expected 100 records, got %d", kv.Count)
	}
	if kv.More {
		t.Fatalf("expected more==false")
	}
	if string(kv.Kvs[59].Key) != pfx+"0059" {
		t.Fatalf("expected different key, got %s", kv.Kvs[9].Key)
	}
	kv, err = em.ec.Get(ctx, pfx+"0001", etcd.WithLimit(100), etcd.WithRange(etcd.GetPrefixRangeEnd(pfx)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(kv.Kvs) != 99 {
		t.Fatalf("expected 99 records, got %d", kv.Count)
	}
	if kv.More {
		t.Fatalf("expected more==false")
	}
	if string(kv.Kvs[59].Key) != pfx+"0060" {
		t.Fatalf("expected different key, got %s", kv.Kvs[9].Key)
	}
}
func BenchmarkGetStreamInfo(b *testing.B) {
	ctx, em := helperGetEM(nil)
	uu := uuid.NewRandom()
	col := fmt.Sprintf("test.%x", uu)
	err := em.CreateStream(ctx, uu, col, nil, nil)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := em.GetStreamInfo(ctx, uu)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}
func BenchmarkGetStreamInfo2(b *testing.B) {
	ctx, em := helperGetEM(nil)
	uu := uuid.NewRandom()
	col := fmt.Sprintf("test.%x", uu)
	err := em.CreateStream(ctx, uu, col, nil, nil)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := em.GetStreamInfo2(ctx, uu)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestCursorBasic(t *testing.T) {
	ctx, em := helperGetEM(nil)
	for i := 0; i < 10; i++ {
		uu := uuid.NewRandom()
		col := "col.abc"
		err := em.CreateStream(ctx, uu, col, map[string]string{"foo": fmt.Sprintf("%03d", i)}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	cursor, err := em.openCursor(ctx, "t", "col.abc", false, "foo", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	count := 0
	for {
		head, fin, err := cursor.pop()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		fmt.Printf("found: %x (%d) fin=%v\n", head, count, fin)
		if fin {
			break
		}

		count++
	}
	if count != 10 {
		t.Fatalf("expected 10 records, got %d", count)
	}
}

func TestCursorVMatch(t *testing.T) {
	ctx, em := helperGetEM(nil)
	for i := 0; i < 32; i++ {
		uu := uuid.NewRandom()
		col := "col.abc"
		err := em.CreateStream(ctx, uu, col, map[string]string{"foo": fmt.Sprintf("%03d", i%2), "bar": fmt.Sprintf("%03d", i)}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	val := "001"
	cursor, err := em.openCursor(ctx, "t", "col.abc", false, "foo", &val)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	count := 0
	for {
		head, fin, err := cursor.pop()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		fmt.Printf("found: %x (%d) fin=%v\n", head, count, fin)
		if fin {
			break
		}

		count++
	}
	if count != 16 {
		t.Fatalf("expected 16 records, got %d", count)
	}
}

func TestCursorVMatchHead(t *testing.T) {
	ctx, em := helperGetEM(nil)
	for i := 0; i < 32; i++ {
		uu := uuid.NewRandom()
		col := "col.abc"
		err := em.CreateStream(ctx, uu, col, map[string]string{"foo": fmt.Sprintf("%03d", i%2), "bar": fmt.Sprintf("%03d", i)}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	val := "001"
	cursor, err := em.openCursor(ctx, "t", "col.abc", false, "foo", &val)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	count := 0
	for {
		head, fin, err := cursor.pop()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		fmt.Printf("found: %x (%d) fin=%v\n", head, count, fin)
		if fin {
			nhead, nfin, err := cursor.head()
			if nhead != nil {
				t.Fatalf("expected nil nhead")
			}
			if !nfin {
				t.Fatalf("expected true nfin")
			}
			if err != nil {
				t.Fatalf("unexpected error")
			}
			break
		}
		nhead, nfin, err := cursor.head()
		if !bytes.Equal(nhead, head) {
			t.Fatalf("expected head to be equal instead %x vs %x", nhead, head)
		}
		if nfin != fin {
			t.Fatalf("expected fin equal")
		}
		count++
	}
	if count != 16 {
		t.Fatalf("expected 16 records, got %d", count)
	}
}

func helperCountResults(t *testing.T, lr chan *LookupResult, err chan bte.BTE, expected int) {
	cnt := 0
	for {
		select {
		case le, ok := <-lr:
			if !ok {
				if cnt == expected {
					return
				} else {
					t.Fatalf("expected %d results, got %d", expected, cnt)
				}
			}
			if le == nil {
				t.Fatalf("Got nil result, did not expect")
			}
			cnt++
		case e, ok := <-err:
			if !ok {
				t.Fatalf("did not expect error channel to close")
			}
			t.Fatalf("unexpected error in hcr: %v", e)
		}
	}
}

func TestLookup(t *testing.T) {
	ctx, em := helperGetEM(nil)
	for i := 0; i < 32; i++ {
		uu := uuid.NewRandom()
		col := "col.abc"
		f := fmt.Sprintf("%03d", i%2)
		b := fmt.Sprintf("%03d", i)
		err := em.CreateStream(ctx, uu, col, map[string]string{"foo": f, "bar": b}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

	}
	for i := 0; i < 32; i++ {
		uu := uuid.NewRandom()
		col := "col.abc.d"
		err := em.CreateStream(ctx, uu, col, map[string]string{"foo": fmt.Sprintf("%03d", i%2), "bar": fmt.Sprintf("%03d", i)}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

	}
	fmt.Printf("create done\n")

	clr, cerr := em.LookupStreams(ctx, "col.abc", false, map[string]*string{"foo": nil}, nil)
	helperCountResults(t, clr, cerr, 32)
	clr, cerr = em.LookupStreams(ctx, "col.abc", true, map[string]*string{"foo": nil}, nil)
	helperCountResults(t, clr, cerr, 64)
	one := "001"
	clr, cerr = em.LookupStreams(ctx, "col.abc", false, map[string]*string{"foo": &one}, nil)
	helperCountResults(t, clr, cerr, 16)
	clr, cerr = em.LookupStreams(ctx, "col.abc", true, map[string]*string{"foo": &one}, nil)
	helperCountResults(t, clr, cerr, 32)
	bar := "003"
	clr, cerr = em.LookupStreams(ctx, "col.", true, map[string]*string{"foo": &one, "bar": &bar}, nil)
	helperCountResults(t, clr, cerr, 2)
	clr, cerr = em.LookupStreams(ctx, "col.", true, map[string]*string{"foo": &one, "bar": nil}, nil)
	helperCountResults(t, clr, cerr, 32)

}

// func TestCreateDuplicateUUID(t *testing.T) {
//
// }
// func TestCreateDuplicateTags(t *testing.T) {
//
// }
// func ListCollections(t *testing.T) {
//
// }
