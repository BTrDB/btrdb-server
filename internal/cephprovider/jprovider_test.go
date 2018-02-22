package cephprovider

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/BTrDB/btrdb-server/internal/configprovider"
	"github.com/BTrDB/btrdb-server/internal/jprovider"
	"github.com/ceph/go-ceph/rados"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mkjp(t testing.TB) (jprovider.JournalProvider, string) {
	conn, _ := rados.NewConn()
	conn.ReadDefaultConfigFile()
	err := conn.Connect()
	if err != nil {
		t.Fatalf("rados connection: %v", err)
	}
	nodename := uuid.NewRandom().String()
	jp, err := newJournalProvider(nodename, conn, "btrdbhot")
	require.NoError(t, err)
	return jp, nodename
}

// type JournalProvider interface {
// 	//Used by a node that is inserting
// 	Insert(ctx context.Context, rng *configprovider.MashRange, jr *JournalRecord) (checkpoint Checkpoint, err error)
// 	WaitForCheckpoint(ctx context.Context, checkpoint Checkpoint) error
// 	ObtainNodeJournals(ctx context.Context, nodename string) (JournalIterator, error)
// 	ReleaseJournalEntries(ctx context.Context, nodename string, upto Checkpoint, rng *configprovider.MashRange) error
// 	ForgetAboutNode(ctx context.Context, nodename string) error
// }

// type JournalRecord struct {
// 	//The stream UUID
//
// 	//The version of the stream that this would appear in
// 	MajorVersion uint64 `msgpack:"f"`
// 	//The microversion that this represents
// 	MicroVersion uint32 `msgpack:"m"`
// 	//Data point times
// 	Times []int64 `msgpack:"t"`
// 	//Data point valuez
// 	Values []float64 `msgpack:"v"`
// }

func makeJRZ(number int, each int) []*jprovider.JournalRecord {
	rv := make([]*jprovider.JournalRecord, number)
	uu := []byte(uuid.NewRandom())
	for i := 0; i < number; i++ {
		tz := make([]int64, each)
		vz := make([]float64, each)
		for k := 0; k < each; k++ {
			tz[k] = time.Now().UnixNano()
			vz[k] = float64(time.Now().UnixNano())
		}
		rv[i] = &jprovider.JournalRecord{
			UUID:         uu,
			MajorVersion: uint64(i),
			MicroVersion: 13,
			Times:        tz,
			Values:       vz,
		}
	}
	return rv
}
func TestInsertAndReadback(t *testing.T) {
	jp, nn := mkjp(t)
	jrz := makeJRZ(1, 30)
	ctx, ctxcancel := context.WithCancel(context.Background())
	defer ctxcancel()
	fullrange := configprovider.MashRange{Start: 0, End: configprovider.HASHRANGE_END}
	fmt.Printf("test calling insert\n")
	cp, err := jp.Insert(ctx, &fullrange, jrz[0])
	if cp != 1 {
		t.Fatalf("Expected checkpoint 1, got %d", cp)
	}
	require.NoError(t, err)
	err = jp.WaitForCheckpoint(ctx, cp)
	require.NoError(t, err)
	//Iterate and check they are all there
	iter, err := jp.ObtainNodeJournals(ctx, nn)
	require.NoError(t, err)
	found := false
	counter := 0
	for iter.Next() {
		rec, cp, err := iter.Value()
		require.NoError(t, err)
		require.EqualValues(t, jrz[0], rec)
		require.EqualValues(t, cp, 1)
		found = true
		counter++
	}
	if !found {
		t.Fatalf("No readback records found")
	}
	if counter > 1 {
		t.Fatalf("Should only have got one result")
	}
	jp.ForgetAboutNode(ctx, nn)
}

func TestInsertAndSingleRangeRelease(t *testing.T) {
	jp, nn := mkjp(t)
	jrz := makeJRZ(1, 30)
	ctx, ctxcancel := context.WithCancel(context.Background())
	defer ctxcancel()

	fullrange := configprovider.MashRange{Start: 0, End: configprovider.HASHRANGE_END}
	cp, err := jp.Insert(ctx, &fullrange, jrz[0])
	if cp != 1 {
		t.Fatalf("Expected checkpoint 1, got %d", cp)
	}
	require.NoError(t, err)
	err = jp.Barrier(ctx, cp)
	require.NoError(t, err)
	//Checkpoint is exlusive - we need +1
	err = jp.ReleaseJournalEntries(ctx, nn, cp+1, &fullrange)
	require.NoError(t, err)

	iter, err := jp.ObtainNodeJournals(ctx, nn)
	require.NoError(t, err)
	counter := 0
	for iter.Next() {
		counter++
	}
	if counter != 0 {
		t.Fatalf("Zero readback record should have been found")
	}
}

func TestObtainOrdering(t *testing.T) {
	jp, nn := mkjp(t)
	jrz := makeJRZ(500, 30)
	ctx, ctxcancel := context.WithCancel(context.Background())
	defer ctxcancel()

	fullrange := configprovider.MashRange{Start: 0, End: configprovider.HASHRANGE_END}

	var cp jprovider.Checkpoint
	for _, jr := range jrz {
		var err error
		cp, err = jp.Insert(ctx, &fullrange, jr)
		require.NoError(t, err)
	}
	err := jp.WaitForCheckpoint(ctx, cp)
	require.NoError(t, err)

	expected := 1
	iter, err := jp.ObtainNodeJournals(ctx, nn)
	require.NoError(t, err)
	for iter.Next() {
		_, cp, err := iter.Value()
		require.Equal(t, jprovider.Checkpoint(expected), cp)
		require.NoError(t, err)
		expected++
	}
	if expected != 501 {
		t.Fatalf("We should have read 500 records")
	}
}

func TestDuplicateNodeAndForget(t *testing.T) {
	conn, _ := rados.NewConn()
	conn.ReadDefaultConfigFile()
	conn.Connect()
	ctx, ctxcancel := context.WithCancel(context.Background())
	defer ctxcancel()
	_, err := conn.OpenIOContext("btrdbhot")
	require.NoError(t, err)

	nodename := uuid.NewRandom().String()
	jp, err := newJournalProvider(nodename, conn, "btrdbhot")
	require.NoError(t, err)
	_, err = newJournalProvider(nodename, conn, "btrdbhot")
	require.Error(t, err)
	jp.ForgetAboutNode(ctx, nodename)
	_, err = newJournalProvider(nodename, conn, "btrdbhot")
	require.NoError(t, err)
}

func TestRadosOutOfOrder(t *testing.T) {
	conn, _ := rados.NewConn()
	conn.ReadDefaultConfigFile()
	err := conn.Connect()
	require.NoError(t, err)
	ioctx, err := conn.OpenIOContext("btrdbhot")
	require.NoError(t, err)
	uus := uuid.NewRandom().String()
	err = ioctx.Write(uus, []byte("hello"), 200)
	require.NoError(t, err)
}
func TestInsertAndMultiRangeRelease(t *testing.T) {
	jp, nn := mkjp(t)
	jrz := makeJRZ(1, 30)
	ctx, ctxcancel := context.WithCancel(context.Background())
	defer ctxcancel()
	rA := int64(0)
	rB := int64(0x40123456)
	rC := int64(0x80123456)
	rD := int64(0xc0123456)
	rE := int64(configprovider.HASHRANGE_END)

	fullrange := configprovider.MashRange{Start: 0, End: configprovider.HASHRANGE_END}
	cp, err := jp.Insert(ctx, &fullrange, jrz[0])
	if cp != 1 {
		t.Fatalf("Expected checkpoint 1, got %d", cp)
	}
	require.NoError(t, err)
	err = jp.Barrier(ctx, cp)
	require.NoError(t, err)

	hasEntries := func() bool {
		iter, err := jp.ObtainNodeJournals(ctx, nn)
		require.NoError(t, err)
		found := false
		counter := 0
		for iter.Next() {
			found = true
			counter++
		}
		return found
	}

	err = jp.ReleaseJournalEntries(ctx, nn, cp+1, &configprovider.MashRange{Start: rA, End: rC})
	require.NoError(t, err)
	require.Equal(t, true, hasEntries())

	err = jp.ReleaseJournalEntries(ctx, nn, cp+1, &configprovider.MashRange{Start: rD, End: rE})
	require.NoError(t, err)
	require.Equal(t, true, hasEntries())

	err = jp.ReleaseJournalEntries(ctx, nn, cp+1, &configprovider.MashRange{Start: rB, End: rD})
	require.NoError(t, err)
	require.Equal(t, false, hasEntries())

}

func Test10KNoFlush(t *testing.T) {

	jrz := makeJRZ(10000, 1000)
	fullrange := configprovider.MashRange{Start: 0, End: configprovider.HASHRANGE_END}

	var cp jprovider.Checkpoint

	jp, nn := mkjp(t)
	ctx, ctxcancel := context.WithCancel(context.Background())
	for _, jr := range jrz {
		var err error

		cp, err = jp.Insert(ctx, &fullrange, jr)
		require.NoError(t, err)
	}
	err := jp.WaitForCheckpoint(ctx, cp)
	require.NoError(t, err)

	expected := 1
	iter, err := jp.ObtainNodeJournals(ctx, nn)
	require.NoError(t, err)
	for iter.Next() {
		_, cp, err := iter.Value()
		require.Equal(t, jprovider.Checkpoint(expected), cp)
		require.NoError(t, err)
		expected++
	}
	if expected != 10001 {
		t.Fatalf("We should have read 10k records, we got %d", expected)
	}
	ctxcancel()
}

//
// func TestJPList1(t *testing.T) {
// 	jpi, _ := mkjp(t)
// 	jp := jpi.(*CJournalProvider)
// 	jp.addFreedSegmentToList(1, 5)
// 	jp.addFreedSegmentToList(10, 20)
// 	jp.addFreedSegmentToList(5, 10)
// 	assert.EqualValues(t, 1, jp.fsHead.Start)
// 	assert.EqualValues(t, 20, jp.fsHead.End)
// 	assert.EqualValues(t, 1, len(jp.freeSegList()))
// }
//
// func TestJPList4(t *testing.T) {
// 	jpi, _ := mkjp(t)
// 	jp := jpi.(*CJournalProvider)
// 	jp.addFreedSegmentToList(5, 10)
// 	jp.addFreedSegmentToList(1, 5)
// 	jp.addFreedSegmentToList(10, 20)
// 	jp.addFreedSegmentToList(25, 25)
// 	assert.EqualValues(t, 1, jp.fsHead.Start)
// 	assert.EqualValues(t, 20, jp.fsHead.End)
// 	assert.EqualValues(t, 1, len(jp.freeSegList()))
// }
//
// func TestJPList2(t *testing.T) {
// 	jpi, _ := mkjp(t)
// 	jp := jpi.(*CJournalProvider)
//
// 	jp.addFreedSegmentToList(5, 10)
// 	jp.addFreedSegmentToList(1, 4)
// 	jp.addFreedSegmentToList(12, 20)
// 	jp.addFreedSegmentToList(4, 5)
// 	jp.addFreedSegmentToList(10, 12)
// 	assert.EqualValues(t, 1, jp.fsHead.Start)
// 	assert.EqualValues(t, 20, jp.fsHead.End)
// 	assert.EqualValues(t, 1, len(jp.freeSegList()))
// }
//
// func TestJPList3(t *testing.T) {
// 	jpi, _ := mkjp(t)
// 	jp := jpi.(*CJournalProvider)
//
// 	jp.addFreedSegmentToList(5, 10)
// 	jp.addFreedSegmentToList(1, 4)
// 	jp.addFreedSegmentToList(12, 20)
// 	jp.addFreedSegmentToList(4, 5)
// 	assert.EqualValues(t, 1, jp.fsHead.Start)
// 	assert.EqualValues(t, 10, jp.fsHead.End)
// 	assert.EqualValues(t, 2, len(jp.freeSegList()))
// 	assert.EqualValues(t, 12, jp.fsHead.Next.Start)
// 	assert.EqualValues(t, 20, jp.fsHead.Next.End)
// }

func TestJPE2EInOrder(t *testing.T) {
	jp, nn := mkjp(t)
	jrz := makeJRZ(4, 30)
	ctx, ctxcancel := context.WithCancel(context.Background())
	defer ctxcancel()

	fullrange := configprovider.MashRange{Start: 0, End: configprovider.HASHRANGE_END}
	var cp jprovider.Checkpoint
	for i := 0; i < 4; i++ {
		var err error
		cp, err = jp.Insert(ctx, &fullrange, jrz[i])
		require.NoError(t, err)
		err = jp.Barrier(ctx, cp)
		require.NoError(t, err)
	}
	count := func() int {
		iter, err := jp.ObtainNodeJournals(ctx, nn)
		require.NoError(t, err)
		counter := 0
		for iter.Next() {
			counter++
		}
		return counter
	}
	if count() != 4 {
		t.Fatalf("Four readback records should have been found")
	}
	err := jp.ReleaseDisjointCheckpoint(ctx, 1)
	require.NoError(t, err)
	if c := count(); c != 3 {
		t.Fatalf("Three readback records should have been found, got %d", c)
	}
	err = jp.ReleaseDisjointCheckpoint(ctx, 2)
	require.NoError(t, err)
	err = jp.ReleaseDisjointCheckpoint(ctx, 3)
	require.NoError(t, err)
	if c := count(); c != 1 {
		t.Fatalf("One readback records should have been found, got %d", c)
	}
	err = jp.ReleaseDisjointCheckpoint(ctx, 4)
	require.NoError(t, err)
	if c := count(); c != 0 {
		t.Fatalf("No readback records should have been found, got %d", c)
	}
}

func TestJPE2EOutOfOrder(t *testing.T) {
	jp, nn := mkjp(t)
	jrz := makeJRZ(4, 30)
	ctx, ctxcancel := context.WithCancel(context.Background())
	defer ctxcancel()

	fullrange := configprovider.MashRange{Start: 0, End: configprovider.HASHRANGE_END}
	var cp jprovider.Checkpoint
	for i := 0; i < 4; i++ {
		var err error
		cp, err = jp.Insert(ctx, &fullrange, jrz[i])
		require.NoError(t, err)
		err = jp.Barrier(ctx, cp)
		require.NoError(t, err)
	}
	count := func() int {
		iter, err := jp.ObtainNodeJournals(ctx, nn)
		require.NoError(t, err)
		counter := 0
		for iter.Next() {
			counter++
		}
		return counter
	}
	if count() != 4 {
		t.Fatalf("Four readback records should have been found")
	}
	err := jp.ReleaseDisjointCheckpoint(ctx, 4)
	require.NoError(t, err)
	assert.EqualValues(t, 4, count())
	err = jp.ReleaseDisjointCheckpoint(ctx, 1)
	assert.EqualValues(t, 3, count())
	err = jp.ReleaseDisjointCheckpoint(ctx, 2)
	require.NoError(t, err)
	err = jp.ReleaseDisjointCheckpoint(ctx, 3)
	require.NoError(t, err)
	assert.EqualValues(t, 0, count())
}

func Test10KFreeForAll(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	writehook = func() {
		time.Sleep((time.Duration(rand.Int()) % 200) * time.Millisecond)
	}
	defer func() { writehook = func() {} }()

	wg := sync.WaitGroup{}
	wg.Add(10)
	jp, _ := mkjp(t)
	for i := 0; i < 10; i++ {
		go func() {
			fmt.Printf("building JRZ\n")
			jrz := makeJRZ(10000, 1000)
			fullrange := configprovider.MashRange{Start: 0, End: configprovider.HASHRANGE_END}
			var cp jprovider.Checkpoint
			fmt.Printf("inserting\n")
			ctx, ctxcancel := context.WithCancel(context.Background())
			for _, jr := range jrz {
				var err error
				cp, err = jp.Insert(ctx, &fullrange, jr)
				require.NoError(t, err)
			}
			err := jp.WaitForCheckpoint(ctx, cp)
			require.NoError(t, err)
			ctxcancel()
			fmt.Printf("DINe\n")
			wg.Done()
		}()
	}
	fmt.Printf("waiting\n")
	wg.Wait()
	fmt.Printf("wait done\n")
}

func Benchmark10KFreeForAllNoDelay(t *testing.B) {
	fmt.Printf("building JRZ\n")
	jrz := makeJRZ(10000, 1000)
	for n := 0; n < t.N; n++ {
		wg := sync.WaitGroup{}
		wg.Add(10)
		jp, _ := mkjp(t)
		for i := 0; i < 10; i++ {
			go func() {
				fullrange := configprovider.MashRange{Start: 0, End: configprovider.HASHRANGE_END}
				var cp jprovider.Checkpoint
				ctx, ctxcancel := context.WithCancel(context.Background())
				for _, jr := range jrz {
					var err error
					cp, err = jp.Insert(ctx, &fullrange, jr)
					require.NoError(t, err)
				}
				err := jp.WaitForCheckpoint(ctx, cp)
				require.NoError(t, err)
				ctxcancel()
				fmt.Printf("DINe\n")
				wg.Done()
			}()
		}
		fmt.Printf("waiting\n")
		wg.Wait()
		fmt.Printf("wait done\n")
	}
}

//v0: Benchmark1KCPInsert-4   	       1	22'482'029'769 ns/op
//v1:	Flip each time  								 1 	 3`600`360`946 ns/op
//v2: No flip 												1 	 1`118`329`694 ns/op
//v2 on freeside:													 1`178`567`598
//v3: one worker:													 1`201`760`167
func Benchmark1KCPInsert(b *testing.B) {
	jrz := makeJRZ(1000, 30)
	fullrange := configprovider.MashRange{Start: 0, End: configprovider.HASHRANGE_END}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jp, _ := mkjp(b)
		ctx, ctxcancel := context.WithCancel(context.Background())
		for idx, jr := range jrz {
			cp, err := jp.Insert(ctx, &fullrange, jr)
			require.NoError(b, err)
			if idx%10 == 0 {
				err := jp.WaitForCheckpoint(ctx, cp)
				err = jp.WaitForCheckpoint(ctx, cp)
				require.NoError(b, err)
			}
		}
		ctxcancel()
	}
}

func Benchmark1KCPInsertAlt(b *testing.B) {
	jrz := makeJRZ(10000, 30)
	fullrange := configprovider.MashRange{Start: 0, End: configprovider.HASHRANGE_END}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jp, _ := mkjp(b)
		ctx, ctxcancel := context.WithCancel(context.Background())
		for idx, jr := range jrz {
			cp, err := jp.Insert(ctx, &fullrange, jr)
			require.NoError(b, err)
			if idx%10 == 0 {
				go jp.WaitForCheckpoint(ctx, cp)

			}
		}
		ctxcancel()
	}
}

func Benchmark10KCPInsertAlt(b *testing.B) {
	jrz := makeJRZ(10000, 30)
	fullrange := configprovider.MashRange{Start: 0, End: configprovider.HASHRANGE_END}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jp, _ := mkjp(b)
		ctx, ctxcancel := context.WithCancel(context.Background())
		for idx, jr := range jrz {
			cp, err := jp.Insert(ctx, &fullrange, jr)
			require.NoError(b, err)
			if idx%1000 == 0 {
				go jp.WaitForCheckpoint(ctx, cp)

			}
		}
		ctxcancel()
	}
}
