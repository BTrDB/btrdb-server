
package quasar

import (
	"testing"
	"code.google.com/p/go-uuid/uuid"
	"cal-sdb.org/quasar/qtree"
	"log"
)

func TestMultInsert(t *testing.T) {
	testuuid := uuid.NewRandom()
	q, err := NewQuasar(&DefaultQuasarConfig)
	if err != nil {
		log.Panic(err)
	}
	vals := []qtree.Record {{10,10},{20,20}}
	log.Printf("a")
	q.InsertValues(testuuid, vals)
	log.Printf("b")
	q.InsertValues(testuuid, vals)
	log.Printf("c")
}