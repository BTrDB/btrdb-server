package bstore

import (
	"github.com/op/go-logging"
)

var log *logging.Logger

func init() {
	log = logging.MustGetLogger("log")
}

const (
	VSIZE = 1024	
	KFACTOR = 64
	VBSIZE = 2+9*VSIZE + 9*VSIZE + 2*VSIZE //Worst case with huffman
	CBSIZE = 1 + KFACTOR*9*6
	DBSIZE = VBSIZE
	PWFACTOR = uint8(6) //1<<6 == 64
)