package main

import (
	"fmt"
	"time"

	"github.com/pborman/uuid"
	"github.com/urfave/cli"
	"gopkg.in/cheggaaa/pb.v1"
)

type StandardValue struct {
	Time int64
	Val  float64
}
type Provider interface {
	Count(uuid uuid.UUID, after, before int64) (uint64, error)
	Delete(uuid uuid.UUID, after, before int64) error
	Query(uuid uuid.UUID, after, before int64) chan StandardValue
}

func newV4Provider(ip string) Provider {
	panic("not implemented lol")
}

func actionCopy(c *cli.Context) error {
	if c.String("v3src") == "" && c.String("src") == "" {
		return cli.NewExitError("either v3src or src must be specified", 1)
	}
	srcip := c.String("src")
	srclegacy := false
	if srcip == "" {
		srcip = c.String("v3src")
		srclegacy = true
	}
	if c.String("v3dst") == "" && c.String("dst") == "" {
		return cli.NewExitError("either v3dst or dst must be specified", 2)
	}
	var after int64 = -(16 << 56)
	var before int64 = (48 << 56)
	if c.String("after") != "" {
		time, error := time.Parse(time.RFC3339, c.String("after"))
		if error != nil {
			return cli.NewExitError("could not parse 'after' time", 4)
		}
		after = time.UnixNano()
	}
	if c.String("before") != "" {
		time, error := time.Parse(time.RFC3339, c.String("before"))
		if error != nil {
			return cli.NewExitError("could not parse 'before' time", 4)
		}
		before = time.UnixNano()
	}
	dstip := c.String("dst")
	dstlegacy := false
	if dstip == "" {
		dstip = c.String("v3dst")
		dstlegacy = true
	}
	if c.String("uuid") == "" {
		return cli.NewExitError("uuid must be specified", 3)
	}
	srcuu := uuid.Parse(c.String("uuid"))
	if srcuu == nil {
		return cli.NewExitError("could not parse uuid", 3)
	}
	var dstuuid uuid.UUID
	if c.String("dstuuid") == "same" {
		dstuuid = srcuu
	} else {
		dstuuid = uuid.Parse(c.String("dstuuid"))
		if dstuuid == nil {
			return cli.NewExitError("invalid dstuuid", 3)
		}
	}
	var SrcP Provider
	if srclegacy {
		SrcP = newV3Provider(srcip)
	} else {
		SrcP = newV4Provider(srcip)
	}
	var DstP Provider
	if dstlegacy {
		DstP = newV3Provider(dstip)
	} else {
		DstP = newV4Provider(dstip)
	}
	if c.Bool("delete") {
		err := DstP.Delete(dstuuid, after, before)
		if err != nil {
			return cli.NewExitError(fmt.Sprintf("Could not delete destination range: %s", err.Error()), 10)
		}
	}
	//First estimate the number of points
	count, err := SrcP.Count(srcuu, after, before)
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("Could not query src: %s", err.Error()), 10)
	}
	fmt.Printf("There are %d points\n", count)
	bar := pb.StartNew(int(count))
	//bar.Postfix(srcuu.String())
	vch := SrcP.Query(srcuu, after, before)
	for v := range vch {
		bar.Increment()
		_ = v
	}
	//	for block := SrcP
	return nil
}
