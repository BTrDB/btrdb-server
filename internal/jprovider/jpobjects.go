// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

package jprovider

//go:generate msgp -tests=false

type JournalRecord struct {
	//The stream UUID
	UUID []byte `msgpack:"u"`
	//The version of the stream that this would appear in
	MajorVersion uint64 `msgpack:"f"`
	//The microversion that this represents
	MicroVersion uint32 `msgpack:"m"`
	//Data point times
	Times []int64 `msgpack:"t"`
	//Data point valuez
	Values []float64 `msgpack:"v"`
}
