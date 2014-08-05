package main 

import (
	_ "fmt"
	"cal-sdb.org/quasar"
	"log"
	//"code.google.com/p/go-uuid/uuid"
)

func main() {
	q, err := quasar.NewQuasar(&quasar.DefaultQuasarConfig)
	if err != nil {
		log.Panic(err)
	}
	quasar.QuasarServeHTTP(q, ":3000")
}

