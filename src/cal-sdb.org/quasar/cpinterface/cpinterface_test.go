package cpinterface

import (
	"testing"
)

func TestEncDec(t *testing.T) {
	buf := EncodeMsg()
	DecodeMsg(buf)
}
