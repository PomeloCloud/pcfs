package server

import (
	"context"
	"errors"
	pb "github.com/PomeloCloud/pcfs/proto"
	"github.com/dgraph-io/badger"
	"log"
	"fmt"
)

func (s *PCFSServer) GetBlock(ctx context.Context, req *pb.GetBlockRequest) (*pb.BlockData, error) {
	var res *pb.BlockData
	if err := s.BFTRaft.DB.View(func(txn *badger.Txn) error {
		if bd, err := GetBlockData(txn, req.Group, req.File, req.Index); err == nil {
			res = bd
		} else {
			return err
		}
		return nil
	}); err == nil {
		return res, nil
	} else {
		msg := fmt.Sprint("cannot get the block", err)
		log.Println(msg)
		return nil, errors.New(msg)
	}
}

func (s *PCFSServer) GetFileMeta(ctx context.Context, req *pb.GetFileRequest) (*pb.FileMeta, error) {
	var res *pb.FileMeta
	if err := s.BFTRaft.DB.View(func(txn *badger.Txn) error {
		if bd, err := GetFile(txn, req.Group, req.File); err == nil {
			res = bd
		} else {
			return err
		}
		return nil
	}); err == nil {
		return res, nil
	} else {
		msg := fmt.Sprint("cannot get the file: ", err)
		log.Println(msg)
		return nil, errors.New(msg)
	}
}
