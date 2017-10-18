package server

import (
	bft "github.com/PomeloCloud/BFTRaft4go/server"
)

func ComposeKeyPrefix(t uint32) []byte {
	return append(bft.ComposeKeyPrefix(5, t))
}