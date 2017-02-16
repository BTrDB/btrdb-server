package mprovider

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/SoftwareDefinedBuildings/btrdb/bte"
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
	pfx := fmt.Sprintf("%x", uuid.NewRandom())
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

// func TestCreateDuplicateUUID(t *testing.T) {
//
// }
// func TestCreateDuplicateTags(t *testing.T) {
//
// }
// func ListCollections(t *testing.T) {
//
// }
