package main

import (
	"os"
	bstore "cal-sdb.org/quasar/bstoreGen1"
	"log"
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
func main () {
	switch(os.Args[1]) {
		case "inspect":
		act_inspect(os.Args[2])
	}
}