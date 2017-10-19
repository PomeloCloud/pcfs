package server

import (
	bft "github.com/PomeloCloud/BFTRaft4go/server"
	"github.com/patrickmn/go-cache"
)

type PCFSServer struct {
	BFTRaft       *bft.BFTRaftServer
	PendingBlocks *cache.Cache
}
