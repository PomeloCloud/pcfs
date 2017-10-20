package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/PomeloCloud/BFTRaft4go/utils"
	pb "github.com/PomeloCloud/pcfs/proto"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	"log"
	"strings"
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

func (s *PCFSServer) GetDirectory(ctx context.Context, req *pb.GetDirectoryRequest) (*pb.Directory, error) {
	var res *pb.Directory
	if err := s.BFTRaft.DB.View(func(txn *badger.Txn) error {
		if bd, err := GetDirectory(txn, req.Group, req.Key); err == nil {
			res = bd
		} else {
			return err
		}
		return nil
	}); err == nil {
		return res, nil
	} else {
		msg := fmt.Sprint("cannot get the dir: ", err)
		log.Println(msg)
		return nil, errors.New(msg)
	}
}

// naive implementation
func (s *PCFSServer) ListDirectory(ctx context.Context, req *pb.ListDirectoryRequest) (*pb.ListDirectoryResponse, error) {
	addrParts := strings.Split(req.Path, "/")
	group := req.Group
	volumeName := addrParts[0]
	volumeKey := IdFromName(volumeName)
	res := &pb.ListDirectoryResponse{}
	if err := s.BFTRaft.DB.View(func(txn *badger.Txn) error {
		volume, err := GetVolume(txn, group, volumeKey)
		if err != nil {
			log.Println("cannot get volume for list dir")
			return err
		}
		parentDirKey := volume.RootDir
	ADDRPART:
		for i := 1; i < len(addrParts); i++ {
			parentDir, err := GetDirectory(txn, group, parentDirKey)
			if err != nil {
				log.Println("cannot get dir:", err)
				return err
			}
			for _, file := range parentDir.Files {
				if file[0] == byte(pb.DirectoryItem_DIR) {
					dirKey := file[1:]
					subDir, err := GetDirectory(txn, group, dirKey)
					if err != nil {
						log.Println("cannot iter over parent, missing sub:", subDir, "error:", err)
						continue
					}
					if subDir.Name == addrParts[i] {
						parentDirKey = subDir.Key
						continue ADDRPART
					}
				}
			}
			return errors.New("cannot find dir")
		}
		dir, err := GetDirectory(txn, group, parentDirKey)
		if err != nil {
			return errors.New("cannot get dir for target dir")
		}
		items := []*pb.DirectoryItem{}
		for _, file := range dir.Files {
			t := file[0]
			k := file[1:]
			var item *pb.DirectoryItem
			switch t {
			case byte(pb.DirectoryItem_DIR):
				subDir, err := GetDirectory(txn, group, k)
				if err != nil {
					return err
				}
				item = &pb.DirectoryItem{
					Type: pb.DirectoryItem_DIR,
					File: &pb.FileMeta{},
					Dir:  subDir,
				}
			case byte(pb.DirectoryItem_FILE):
				subFile, err := GetFile(txn, group, k)
				if err != nil {
					return err
				}
				item = &pb.DirectoryItem{
					Type: pb.DirectoryItem_FILE,
					File: subFile,
					Dir:  &pb.Directory{},
				}
			}
			items = append(items, item)
		}
		res.Key = dir.Key
		res.Items = items
		res.Name = dir.Name
		return nil
	}); err == nil {
		return res, nil
	} else {
		log.Println("error on getting dir:", err)
		return nil, err
	}
}

// Following RPCs are for block stash servers
// These servers are likely have no idea about files, directories and volumes
// To get file meta data, it need to consult the group members
func (s *PCFSServer) AppendToBlock(ctx context.Context, req *pb.AppendToBlockRequest) (*pb.WriteResult, error) {
	group := req.Group
	data := req.Data
	offset := req.Offset
	remains := uint64(len(data))
	blockHash := []byte{}
	if err := s.BFTRaft.DB.Update(func(txn *badger.Txn) error {
		block, err := GetBlockData(txn, group, req.File, req.Index)
		dataIdx := 0
		if err != nil {
			return err
		}
		for i := offset; i < uint32(len(block.Data)); i++ {
			if dataIdx == len(data) {
				break
			}
			block.Data[offset] = data[dataIdx]
			dataIdx++
		}
		remains = uint64(len(data) - dataIdx)
		SetBlock(txn, block)
		blockHash, _ = utils.SHA1Hash(block.Data)
		return nil
	}); err == nil {
		return &pb.WriteResult{
			Succeed:   true,
			Remains:   remains,
			BlockHash: blockHash,
		}, nil
	} else {
		return nil, err
	}
}

func (s *PCFSServer) CreateBlock(ctx context.Context, req *pb.CreateBlockRequest) (*pb.WriteResult, error) {
	group := req.Group
	fileMeta, err := s.GetMajorityFileMeta(group, req.File)
	if err != nil {
		log.Println("cannot get block file meta:", err)
		return nil, err
	}
	blockDBKey := BlockDBKey(group, req.File, req.Index)
	if err := s.BFTRaft.DB.Update(func(txn *badger.Txn) error {
		block := &pb.BlockData{
			Group: req.Group,
			Index: req.Index,
			File:  req.File,
			Data:  make([]byte, fileMeta.BlockSize),
		}
		if _, err := txn.Get(blockDBKey); err != badger.ErrKeyNotFound {
			return errors.New("block already exists")
		} else {
			SetBlock(txn, block)
			return nil
		}
	}); err == nil {
		contract := &pb.ConfirmBlockContract{
			NodeId: s.BFTRaft.Id,
			Index:  req.Index,
			File:   req.File,
			Req:    req,
		}
		contractData, err := proto.Marshal(contract)
		if err != nil {
			panic(err)
		}
		res, err := s.BFTRaft.Client.ExecCommand(group, CONFIRM_BLOCK, contractData)
		if err != nil {
			panic(err) // for debug
		}
		switch (*res)[0] {
		case 1:
			log.Println("confirm block succeed")
			return &pb.WriteResult{
				Succeed:   true,
				Remains:   0,
				BlockHash: []byte{},
			}, nil
		default:
			msg := "confirm block failed:"
			log.Println(msg, res)
			return nil, errors.New(msg)
		}
	} else {
		return &pb.WriteResult{
			Succeed: false,
			Remains: 0,
		}, err
	}
}

func (s *PCFSServer) DeleteBlock(ctx context.Context, req *pb.DeleteBlockRequest) (*pb.WriteResult, error) {
	return nil, errors.New("unimplemented")
}

func (s *PCFSServer) SuggestBlockStash(ctx context.Context, req *pb.Nothing) (*pb.BlockStashSuggestion, error) {

}
