// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

package rez

func DefaultResourceTunables() [][]string {
	rv := [][]string{
		[]string{string(CephHotHandle), "100,100"},
		[]string{string(CephColdHandle), "100,100"},
		[]string{string(ConcurrentOp), "200,100"},
		[]string{string(OpenTrees), "100,100"},
		[]string{string(OpenReadTrees), "100,100"},
		[]string{string(MaximumConnections), "100,100"},
	}
	return rv
}
