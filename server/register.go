package server

import (
	rpbs "github.com/PomeloCloud/BFTRaft4go/proto/server"
	pb "github.com/PomeloCloud/pcfs/proto"
	"github.com/c2h5oh/datasize"
	"github.com/golang/protobuf/proto"
	"log"
)

func (s *PCFSServer) CheckStashGroup(join bool) {
	log.Println("check stash group")
	if !s.BFTRaft.Client.GroupExists(STASH_GROUP) {
		err := s.BFTRaft.NewGroup(&rpbs.RaftGroup{
			Replications: 32,
			Id:           STASH_GROUP,
			Term:         0,
		})
		if err != nil {
			panic(err)
		} else {
			log.Println("created stash group")
		}
	} else {
		log.Println("group stash existed")
		if join {
			log.Println("will join stash reg group")
			s.BFTRaft.NodeJoin(STASH_GROUP)
		}
	}
}

func (s *PCFSServer) RegisterNode(config FileConfig) {
	var c datasize.ByteSize
	err := c.UnmarshalText([]byte(config.Capacity))
	if err != nil {
		panic(err)
	}
	log.Println("trying to register this node as stash with capacity:", c.HumanReadable())
	host := &pb.HostStash{
		HostId:   s.BFTRaft.Id,
		Capacity: c.Bytes(),
		Used:     0,
		Owner:    s.BFTRaft.Id,
	}
	hostData, err := proto.Marshal(host)
	if err != nil {
		panic(err)
	}
	res, err := s.BFTRaft.Client.ExecCommand(STASH_GROUP, REG_STASH, hostData)
	if err != nil {
		panic(err)
	}
	switch (*res)[0] {
	case 0:
		panic("cannot reg stash")
	case 1:
		log.Println("register stash successfully")
	}
}
