package main

import (
	"fmt"

	"github.com/pborman/uuid"

	btrdb "gopkg.in/BTrDB/btrdb.v3"
)

type v3provider struct {
	bc *btrdb.BTrDBConnection
}

func newV3Provider(ip string) Provider {
	bc, err := btrdb.NewBTrDBConnection(ip)
	if err != nil {
		fmt.Println("Could not connect:", err.Error())
	}
	return &v3provider{
		bc: bc,
	}
}

func (v3 *v3provider) Count(uuid uuid.UUID, after, before int64) (uint64, error) {
	sv, gen, errm, err := v3.bc.QueryWindowValues(uuid, after, before, uint64(before-after), 0, 0)
	go func() {
		for _ = range gen {

		}
	}()
	if err != nil {
		return 0, err
	}
	select {
	case r := <-sv:
		return r.Count, nil
	case _ = <-errm:
		return 0, fmt.Errorf("query failed")
	}
}
func (v3 *v3provider) Delete(uuid uuid.UUID, after, before int64) error {
	panic("not supported")
}
func (v3 *v3provider) Query(uuid uuid.UUID, after, before int64) chan StandardValue {
	sv, gen, errmsg, err := v3.bc.QueryStandardValues(uuid, after, before, 0)
	if err != nil {
		panic(err)
	}
	go func() {
		for _ = range gen {

		}
	}()
	go func() {
		for m := range errmsg {
			panic(m)
		}
	}()
	rv := make(chan StandardValue, 100)
	go func() {
		for v := range sv {
			rv <- StandardValue{Time: v.Time, Val: v.Value}
		}
		close(rv)
	}()
	return rv
}
func (v3 *v3provider) Insert(uuid uuid.UUID, sv []btrdb.StandardValue) error {
	return nil
}
