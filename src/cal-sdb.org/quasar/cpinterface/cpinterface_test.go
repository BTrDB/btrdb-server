package cpinterface

import (
	"testing"
	"log"
	"bytes"
)

func TestEncDec(t *testing.T) {
	buf := EncodeMsg()
	b2 := []byte{0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 0, 244, 
				 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
				 0, 0, 0, 0, 0}
	bbuf := bytes.NewBuffer(b2)
	log.Printf("encoded buffer: %+v",buf.Bytes())
	DecodeMsg(bbuf)
}
