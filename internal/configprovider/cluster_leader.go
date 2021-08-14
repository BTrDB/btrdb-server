// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

package configprovider

import "github.com/huichen/murmur"

//This will compare the UUID against the proposed mash (or the active mash if there is no
//proposed mash). It will return true if mutating actions can be taken on the UUID
//such as create delete or insert, and it will return false if we are the wrong
//endpoint for this uuid
//Note v49: I think this logic is risky, if we are busy getting ready for a
// proposed mash, then we do not want to accept any inserts for this stream
// until that mash becomes ACTIVE because we might be recovering journal
// entries for the streams and we will reject hte journal entries if the versions
// in the entires don't match the stream (as would happen if we inserted into it
// before completing recovery).
func (c *etcdconfig) WeHoldWriteLockFor(uuid []byte) bool {
	c.ourRangesMu.RLock()
	as := c.ourActive.Start
	ae := c.ourActive.End
	ps := c.ourProposed.Start
	pe := c.ourProposed.End
	c.ourRangesMu.RUnlock()
	hsh := murmur.Murmur3(uuid[:])
	//Return true iff uuid satisfies both
	return (as <= int64(hsh) && ae > int64(hsh)) &&
		(ps <= int64(hsh) && pe > int64(hsh))
}

func (c *etcdconfig) OurRanges() (active MashRange, proposed MashRange) {
	c.ourRangesMu.RLock()
	defer c.ourRangesMu.RUnlock()
	return c.ourActive, c.ourProposed
}

func (mr *MashRange) SuperSetOfUUID(uuid []byte) bool {
	hsh := murmur.Murmur3(uuid[:])
	return mr.Start <= int64(hsh) && mr.End > int64(hsh)
}
