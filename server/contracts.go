package server

import (
	"errors"
	rpb "github.com/PomeloCloud/BFTRaft4go/proto/server"
	bft "github.com/PomeloCloud/BFTRaft4go/server"
	"github.com/PomeloCloud/BFTRaft4go/utils"
	pb "github.com/PomeloCloud/pcfs/proto"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	"log"
	"bytes"
)

// Beta group contracts

const _10MB = uint32(10 * 1024 * 1024)
const _1KB = uint32(1024)

const (
	VOLUMES   = 1
	DIRECTORY = 2
)

func (s *PCFSServer) NewVolume(arg *[]byte, entry *rpb.LogEntry) []byte {
	group := entry.Command.Group
	volume := &pb.Volume{}
	if err := proto.Unmarshal(*arg, volume); err != nil {
		log.Println("cannot decode volume:", err)
		return []byte{0}
	}
	key := IdFromName(volume.Name)
	volume.Key = key
	if volume.Replications > 32 {
		volume.Replications = 32
	}
	if volume.Replications < 5 {
		volume.Replications = 5
	}
	if volume.BlockSize < _1KB {
		volume.BlockSize = _1KB
	}
	if volume.BlockSize > _10MB {
		volume.BlockSize = _10MB
	}
	dbKey := append(bft.ComposeKeyPrefix(group, VOLUMES), key...)
	rootDirDbKey := append(bft.ComposeKeyPrefix(group, DIRECTORY), key...)
	rootDir := &pb.Directory{
		Key: key, Files: [][]byte{},
	}
	volumeData, err := proto.Marshal(volume)
	if err != nil {
		log.Println("cannot encode volume")
		return []byte{0}
	}
	rootDirData, err := proto.Marshal(rootDir)
	if err != nil {
		log.Println("cannot encode root dir")
		return []byte{1}
	}
	if err := s.BFTRaft.DB.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get(dbKey); err != badger.ErrKeyNotFound {
			return errors.New("volume existed")
		}
		if _, err := txn.Get(rootDirDbKey); err != badger.ErrKeyNotFound {
			return errors.New("volume root dir existed")
		}
		if err := txn.Set(dbKey, volumeData, 0x00); err != nil {
			return err
		}
		if err := txn.Set(rootDirDbKey, rootDirData, 0x00); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Println("insert volume failed:", err)
		return []byte{0}
	} else {
		return []byte{1}
	}
}

func (s *PCFSServer) NewDirectory(arg *[]byte, entry *rpb.LogEntry) []byte {
	group := entry.Command.Group
	newDir := &pb.NewDirectoryContract{}
	if err := proto.Unmarshal(*arg, newDir); err != nil {
		log.Println("cannot decode new dir")
		return []byte{0}
	}
	dir := newDir.Dir
	dir.Key, _ = utils.SHA1Hash(append(newDir.ParentDir, entry.Hash...))
	dir.Files = [][]byte{}
	newDirToken := append([]byte{1}, dir.Key...)
	if err := s.BFTRaft.DB.Update(func(txn *badger.Txn) error {
		parentDir, err := s.GetDirectory(txn, group, newDir.ParentDir)
		if err != nil {
			return err
		}
		// check dir existed
		for _, fileKey := range parentDir.Files {
			if bytes.Equal(fileKey, newDirToken) {
				return errors.New("dir exists")
			}
		}
		parentDir.Files = append(parentDir.Files, newDirToken)
		if err := s.SetDirectory(txn, group, dir); err != nil {
			return err
		}
		if err := s.SetDirectory(txn, group, dir); err != nil {
			return err
		}
		if err := s.SetDirectory(txn, group, parentDir); err != nil {
			return err
		}
		return nil
	}); err == nil {
		log.Println("dir created")
		return []byte{1}
	} else {
		log.Println("cannot create dir:", err)
		return []byte{0}
	}
}
