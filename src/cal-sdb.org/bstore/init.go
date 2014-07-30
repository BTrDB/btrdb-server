package bstore

import (
	"log"
)

func init() {
	log.SetFlags( log.Ldate | log.Lmicroseconds | log.Lshortfile )
}