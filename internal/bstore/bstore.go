package bstore

import (
	"github.com/op/go-logging"
)

var lg *logging.Logger

func init() {
	lg = logging.MustGetLogger("log")
}

//Note to self, if you bump VSIZE such that the max blob goes past 2^16, make sure to adapt
//providers
const (
	VSIZE           = 1024
	KFACTOR         = 64
	VBSIZE          = 2 + 9*VSIZE + 9*VSIZE + 2*VSIZE //Worst case with huffman
	CBSIZE          = 1 + KFACTOR*9*6
	DBSIZE          = VBSIZE
	PWFACTOR        = uint8(6) //1<<6 == 64
	RELOCATION_BASE = 0xFF00000000000000
)
