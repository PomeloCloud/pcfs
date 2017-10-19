package server

import (
	"context"
	"errors"
	pb "github.com/PomeloCloud/pcfs/proto"
	"github.com/dgraph-io/badger"
	"log"
)

func (s *PCFSServer) GetBlock(ctx context.Context, req *pb.GetBlockRequest) (*pb.GetBlockResponse, error) {
	var res *pb.BlockData
	if err := s.BFTRaft.DB.View(func(txn *badger.Txn) error {
		if bd, err := GetBlockData(txn, req.Group, req.File, req.Index); err == nil {
			res = bd
		} else {
			return err
		}
		return nil
	}); err == nil {
		return &pb.GetBlockResponse{
			Data: res,
		}, nil
	} else {
		msg := "cannot get the block"
		log.Println(msg)
		return nil, errors.New(msg)
	}
}
