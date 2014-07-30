package main 

import (
	"fmt"
	"cal-sdb.org/bstore"
	"log"
	//"code.google.com/p/go-uuid/uuid"
)


func test() {
	bs, err := bstore.NewBlockStore("localhost")
	if err != nil{
		log.Panic(err)
	}
	u := [...]byte{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1}
	gen := bs.ObtainGeneration(u)
	fmt.Printf("gen: %+v",gen)
}
func main() {
	test()
}

