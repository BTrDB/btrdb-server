package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/pborman/uuid"

	"github.com/BTrDB/btrdb-server"
	"github.com/BTrDB/btrdb-server/internal/bprovider"
	"github.com/BTrDB/btrdb-server/internal/configprovider"
	"github.com/BTrDB/btrdb-server/qtree"
	"github.com/stretchr/testify/require"
)

var qsr *btrdb.Quasar

func init() {

	fcfg, err1 := configprovider.LoadFileConfig("./btrdb.conf")
	if err1 != nil {
		panic(err1)
	}
	hostname := uuid.NewRandom().String()

	cfg, err := configprovider.LoadEtcdConfig(fcfg, hostname)
	if err != nil {
		fmt.Println("Could not load cluster configuration")
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("CONFIG OKAY!")

	//fmt.Printf("Ensuring database is initialized\n")
	//bstore.CreateDatabase(cfg, false)
	//fmt.Printf("Done\n")

	qsr, err = btrdb.NewQuasar(cfg)
	if err != nil {
		log.Panicf("error: %v", err)
	}
	fmt.Println("QUASAR OKAY!")
	time.Sleep(3 * time.Second)
	//sigchan := make(chan os.Signal, 3)
	//signal.Notify(sigchan, os.Interrupt)
}

func TestInsertWithFlush(t *testing.T) {
	uu := uuid.NewRandom()
	col := uuid.NewRandom().String()
	err := qsr.CreateStream(context.Background(), uu, col, nil, nil)
	require.NoError(t, err)
	r := []qtree.Record{{100, 100.0}}
	maj, min, err := qsr.InsertValues(context.Background(), uu, r)
	require.NoError(t, err)
	require.EqualValues(t, bprovider.SpecialVersionFirst, maj)
	require.EqualValues(t, 1, min)
	err = qsr.Flush(context.Background(), uu)
	require.NoError(t, err)
	fmt.Printf("flush completed ok\n")
	rch, ech, maj, min := qsr.QueryValuesStream(context.Background(), uu, 0, 200, btrdb.LatestGeneration)
	counter := 0
	for v := range rch {
		if v.Time != 100 || v.Val != 100.0 {
			t.Fatalf("wrong result value")
		}
		counter++
	}
	select {
	case e := <-ech:
		t.Fatalf("received error: %v\n", e)
	default:
	}
	require.EqualValues(t, bprovider.SpecialVersionFirst+1, maj)
	require.EqualValues(t, 0, min)
	require.EqualValues(t, 1, counter)
}

func TestInsertWithNoFlush(t *testing.T) {
	uu := uuid.NewRandom()
	col := uuid.NewRandom().String()
	err := qsr.CreateStream(context.Background(), uu, col, nil, nil)
	require.NoError(t, err)
	r := []qtree.Record{{100, 100.0}}
	maj, min, err := qsr.InsertValues(context.Background(), uu, r)
	require.NoError(t, err)
	require.EqualValues(t, bprovider.SpecialVersionFirst, maj)
	require.EqualValues(t, 1, min)
	rch, ech, maj, min := qsr.QueryValuesStream(context.Background(), uu, 0, 200, btrdb.LatestGeneration)
	counter := 0
	for v := range rch {
		if v.Time != 100 || v.Val != 100.0 {
			t.Fatalf("wrong result value")
		}
		counter++
	}
	select {
	case e := <-ech:
		t.Fatalf("received error: %v\n", e)
	default:
	}
	require.EqualValues(t, bprovider.SpecialVersionFirst, maj)
	require.EqualValues(t, 1, min)
	require.EqualValues(t, 1, counter)
}

func TestQueryReturnsBothResults(t *testing.T) {
	uu := uuid.NewRandom()
	col := uuid.NewRandom().String()
	err := qsr.CreateStream(context.Background(), uu, col, nil, nil)
	require.NoError(t, err)
	r := []qtree.Record{{100, 100.0}}
	maj, min, err := qsr.InsertValues(context.Background(), uu, r)
	require.NoError(t, err)
	require.EqualValues(t, bprovider.SpecialVersionFirst, maj)
	require.EqualValues(t, 1, min)
	err = qsr.Flush(context.Background(), uu)
	require.NoError(t, err)
	require.EqualValues(t, bprovider.SpecialVersionFirst+1, maj)
	require.EqualValues(t, 0, min)
	r = []qtree.Record{{105, 105.0}}
	maj2, min2, err := qsr.InsertValues(context.Background(), uu, r)
	rch, ech, maj, min := qsr.QueryValuesStream(context.Background(), uu, 0, 200, btrdb.LatestGeneration)
	counter := 0
	for _ = range rch {
		counter++
	}
	select {
	case e := <-ech:
		t.Fatalf("received error: %v\n", e)
	default:
	}
	require.EqualValues(t, bprovider.SpecialVersionFirst+1, maj2)
	require.EqualValues(t, 1, min2)
	require.EqualValues(t, bprovider.SpecialVersionFirst+1, maj)
	require.EqualValues(t, 1, min)
	require.EqualValues(t, 2, counter)
}
