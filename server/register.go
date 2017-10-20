package server

import (
	pb "github.com/PomeloCloud/pcfs/proto"
	rpbs "github.com/PomeloCloud/BFTRaft4go/proto/server"
	"github.com/c2h5oh/datasize"
	"github.com/golang/protobuf/proto"
	"log"
)

func (s *PCFSServer) CheckStashGroup() {
	log.Println("check stash group")
	if !s.BFTRaft.Client.GroupExists(STASH_REG) {
		err := s.BFTRaft.NewGroup(&rpbs.RaftGroup{
			Replications: 32,
			Id: STASH_REG,
			Term: 0,
		})
		if err != nil {
			panic(err)
		} else {
			log.Println("created stash group")
		}
	} else {
		log.Println("group stash existed")
	}
}

func (s *PCFSServer) RegisterNode(config FileConfig) {
	s.CheckStashGroup()
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
	res, err := s.BFTRaft.Client.ExecCommand(STASH_REG, REG_STASH, hostData)
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
