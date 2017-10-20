package server

import (
	bft "github.com/PomeloCloud/BFTRaft4go/server"
	pb "github.com/PomeloCloud/pcfs/proto"
	"github.com/patrickmn/go-cache"
	"time"
	"github.com/PomeloCloud/BFTRaft4go/utils"
	"log"
)

type PCFSServer struct {
	BFTRaft       *bft.BFTRaftServer
	PendingBlocks *cache.Cache
}

func GetServer(bft *bft.BFTRaftServer) *PCFSServer  {
	fsserver := PCFSServer{
		BFTRaft: bft,
		PendingBlocks: cache.New(5 * time.Minute, 5 * time.Minute),
	}
	log.Println("registering storage services")
	pb.RegisterPCFSServer(utils.GetGRPCServer(bft.Opts.Address), &fsserver)
	return &fsserver
}