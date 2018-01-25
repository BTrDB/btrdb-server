package pqm

import (
	"context"
	"testing"

	"github.com/BTrDB/btrdb-server/bte"
	"github.com/BTrDB/btrdb-server/internal/bprovider"
	"github.com/immesys/rados"
	"github.com/pborman/uuid"
)

type dummySI struct {
	versions map[uuid.Array]uint64
}

func (d *dummySI) StreamMajorVersion(ctx context.Context, id uuid.UUID) (int64, bte.BTE) {
	ver, ok := d.versions[id.Array()]
	if !ok {
		ver = bprovider.SpecialVersionCreated
		d.versions[id.Array()] = ver
	}
	return ver, nil
}
func (d *dummySI) WritePrimaryStorage(ctx context.Context, id uuid.UUID, r []Record) (major int64, err bte.BTE) {
	ver, ok := d.versions[id.Array()]
	if !ok {
		ver = bprovider.SpecialVersionCreated
	}
	ver++
	d.versions[id.Array()] = ver
	return ver, nil
}
func getPQM() *PQM {
	jp := rados.NewConn()
	conn.ReadDefaultConfigFile()
	err := conn.Connect()
	if err != nil {
		panic(err)
	}
	nodename := uuid.NewRandom().String()
	jp, err := newJournalProvider(nodename, conn, "btrdbhot")
	return NewPQM(&dummySI{jp: jp})
}

func TestInsert(t *testing.T) {

}
