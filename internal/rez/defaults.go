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
