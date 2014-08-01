package qtree

import (
	"cal-sdb.org/bstore"
	"log"
	"testing"
)

var _bs *bstore.BlockStore = nil
func mBS() {
	if _bs == nil {
		nbs, err := bstore.NewBlockStore("localhost")
		if err != nil {
			log.Panic(err)
		}
		_bs = nbs
	}
}

var testuuid bstore.UUID = bstore.UUID([...]byte{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2})

func TestTreeWrite(t *testing.T) {
	mBS()
	_bs.DEBUG_DELETE_UUID(testuuid)
	tr, err := NewWriteQTree(_bs, testuuid)
	if err != nil {
		t.Error(err)
	}
	records := []Record{ Record{1,1}, Record{2,2}, Record{3,3} }
	tr.InsertValues(records)
	tr.Commit()
	
	tr, err = NewReadQTree(_bs, testuuid, bstore.LatestGeneration)
	if err != nil {
		log.Panic(err)
	}
	recordc := make(chan Record)
	errc := make(chan error)
	log.Printf("beginning chan select")
	go tr.ReadStandardValuesCI(recordc, errc, -1, 8)
	for {
		select {
			case r, r_c := <- recordc:
				log.Printf("got record (%v) %v", r_c, r)
				if (!r_c) { return }
			case err, err_c := <- errc:
				log.Printf("got error (%v) %v", err_c, err)
				return
		}
	}
}
