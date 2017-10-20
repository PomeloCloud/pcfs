package server

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/PomeloCloud/pcfs/proto"
	"github.com/dgraph-io/badger"
	"log"
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

func (s *PCFSServer) GetVolume(ctx context.Context, req *pb.GetVolumeRequest) (*pb.Volume, error) {
	var res *pb.Volume
	if err := s.BFTRaft.DB.View(func(txn *badger.Txn) error {
		if bd, err := GetVolume(txn, req.Group, IdFromName(req.Name)); err == nil {
			res = bd
		} else {
			return err
		}
		return nil
	}); err == nil {
		return res, nil
	} else {
		msg := fmt.Sprint("cannot get the volume: ", err)
		log.Println(msg)
		return nil, errors.New(msg)
	}
}
