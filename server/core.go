package server

import (
	bft "github.com/PomeloCloud/BFTRaft4go/server"
)

type PCFSServer struct {
	BFTRaft *bft.BFTRaftServer
}