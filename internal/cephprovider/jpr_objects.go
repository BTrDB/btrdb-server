// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

package cephprovider

import "github.com/BTrDB/btrdb-server/internal/jprovider"

//go:generate msgp -tests=false

type CJrecord struct {
	R *jprovider.JournalRecord
	C uint64
}
