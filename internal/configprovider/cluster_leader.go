package configprovider

import "github.com/huichen/murmur"

//This will compare the UUID against the proposed mash (or the active mash if there is no
//proposed mash). It will return true if mutating actions can be taken on the UUID
//such as create delete or insert, and it will return false if we are the wrong
//endpoint for this uuid
func (c *etcdconfig) WeHoldWriteLockFor(uuid []byte) bool {
	c.notifiedRangeMu.RLock()
	s := c.ourNotifiedStart
	e := c.ourNotifiedEnd
	c.notifiedRangeMu.RUnlock()
	hsh := murmur.Murmur3(uuid[:])
	return s <= int64(hsh) && e > int64(hsh)
}

func (c *etcdconfig) OurNotifiedRange() MashRange {
	c.notifiedRangeMu.RLock()
	rv := MashRange{Start: c.ourNotifiedStart, End: c.ourNotifiedEnd}
	c.notifiedRangeMu.RUnlock()
	return rv
}

func (mr *MashRange) SuperSetOfUUID(uuid []byte) bool {
	hsh := murmur.Murmur3(uuid[:])
	return mr.Start <= int64(hsh) && mr.End > int64(hsh)
}
