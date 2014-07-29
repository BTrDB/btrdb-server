package main 

import (
	"fmt"
	"cal-sdb.org/bstore"
	"log"
	//"code.google.com/p/go-uuid/uuid"
)


func test() {
	bs := new (bstore.BlockStore)
	bs.Init("localhost")
	u := [...]byte{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1}
	gen := bs.ObtainGeneration(u)
	fmt.Printf("gen: %+v",gen)
}
func main() {
	bs := new (bstore.BlockStore)
	if err := bs.Init("localhost"); err != nil {
		log.Panicf("Blockstore error %v", err)
	}
	fmt.Printf("Hello world\n")
	test()
}

