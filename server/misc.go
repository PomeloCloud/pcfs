package server

import (
	"github.com/PomeloCloud/BFTRaft4go/utils"
	"log"
)

func (s *PCFSServer) CheckJoinAlphaGroup() {
	if s.BFTRaft.GetOnboardGroup(utils.ALPHA_GROUP) == nil {
		log.Println("will join alpha group")
		s.BFTRaft.NodeJoin(utils.ALPHA_GROUP)
	}
}
