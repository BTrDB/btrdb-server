package main

import (
	"os"
	bstore "github.com/SoftwareDefinedBuildings/quasar/bstoreGen1"
	"github.com/SoftwareDefinedBuildings/quasar"
	"log"
	"code.google.com/p/go-uuid/uuid"
	"fmt"
)

func act_inspect(dbpath string) {
	bs, err := bstore.NewBlockStore("localhost", 0, dbpath)
	if err != nil {
		log.Panic(err)
	}
	alloced, free, strange, leaked := bs.InspectBlocks() 
	log.Printf("SUMMARY:")
	log.Printf("ALLOCED: %d", alloced)
	log.Printf("FREE   : %d", free)
	log.Printf("STRANGE: %d", strange)
	log.Printf("LEAKED : %d", leaked)
	log.Printf("USAGE  : %.2f %%", float64(alloced) / float64(alloced + free) * 100)
}
func act_scrub(dbpath string, sids []string) {
	ids := make([]uuid.UUID,len(sids))
	sgens := make([]uint64,len(sids))
	egens := make([]uint64,len(sids))
	cfg := quasar.DefaultQuasarConfig
	cfg.BlockPath = dbpath
	cfg.DatablockCacheSize = 0
	q, err := quasar.NewQuasar(&cfg) 
	if err != nil {
		log.Panic(err)
	}

	lidx := 0
	for idx, sid := range sids {
		ids[idx] = uuid.Parse(sid)
		if ids[idx] == nil {
			log.Printf("Could not parse UUID")
			os.Exit(1)
		}
		lgen, err := q.QueryGeneration(ids[idx])
		if err != nil {
			fmt.Printf("Could not find a generation for that stream\n")
			continue
		}
		log.Printf("The latest generation for stream %s is %d",ids[idx], lgen)
		if lgen < 3 {
			log.Printf("Not unlinking, not worth it")
			continue
		}
		egens[idx] = lgen-1
		sgens[idx] = 0
		idx++
		lidx = idx
	}
	
	{
		alloced, free, strange, leaked := q.InspectBlocks() 
		log.Printf("Storage before operation:")
		log.Printf("ALLOCED: %d", alloced)
		log.Printf("FREE   : %d", free)
		log.Printf("STRANGE: %d", strange)
		log.Printf("LEAKED : %d", leaked)
		log.Printf("USAGE  : %.2f %%", float64(alloced) / float64(alloced + free) * 100)
	}
	
	err = q.UnlinkBlocks(ids[:lidx], sgens[:lidx], egens[:lidx])
	{
		alloced, free, strange, leaked := q.InspectBlocks() 
		log.Printf("Storage after operation:")
		log.Printf("ALLOCED: %d", alloced)
		log.Printf("FREE   : %d", free)
		log.Printf("STRANGE: %d", strange)
		log.Printf("LEAKED : %d", leaked)
		log.Printf("USAGE  : %.2f %%", float64(alloced) / float64(alloced + free) * 100)
	}
}
func act_freeleaks(dbpath string) {
	cfg := quasar.DefaultQuasarConfig
	cfg.BlockPath = dbpath
	q, err := quasar.NewQuasar(&cfg) 
	if err != nil {
		log.Panic(err)
	}
	q.UnlinkLeaks()
}

func main () {
	switch(os.Args[1]) {
		case "inspect":
			act_inspect(os.Args[2])
		case "scrub":
			act_scrub(os.Args[2], os.Args[3:])
		case "freeleaks":
			act_freeleaks(os.Args[2])
	}
}