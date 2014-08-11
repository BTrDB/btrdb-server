package main

import (
	"os"
	bstore "cal-sdb.org/quasar/bstoreGen1"
	"cal-sdb.org/quasar"
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
func act_scrub(dbpath string, sid string) {
	id := uuid.Parse(sid)
	if id == nil {
		log.Printf("Could not parse UUID")
		os.Exit(1)
	}
	cfg := quasar.DefaultQuasarConfig
	cfg.BlockPath = dbpath
	q, err := quasar.NewQuasar(&cfg) 
	if err != nil {
		log.Panic(err)
	}
	lgen, err := q.QueryGeneration(id)
	if err != nil {
		fmt.Printf("Could not find a generation for that stream\n")
		os.Exit(1)
	}
	log.Printf("The latest generation for stream %s is %d",id, lgen)
	if lgen < 3 {
		log.Printf("Not unlinking, not worth it")
		os.Exit(1)
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
	err = q.UnlinkBlocks(id, 0, lgen-1)
	if err != nil {
		log.Panic(err)
	}
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
			act_scrub(os.Args[2], os.Args[3])
		case "freeleaks":
			act_freeleaks(os.Args[2])
	}
}