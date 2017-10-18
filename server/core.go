package contracts

import (
	pb "github.com/PomeloCloud/pcfs/proto"
)

// Beta group contracts

func (s *s.BFTRaftServer) NewVolume(name string, replications uint32, blockSize uint32) *pb.Volume {
	return nil
}