package server

import (
	"errors"
	rpb "github.com/PomeloCloud/BFTRaft4go/proto/server"
	pb "github.com/PomeloCloud/pcfs/proto"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	"log"
)


// Beta group contracts

const _10MB = uint32(10 * 1024 * 1024)
const _1KB = uint32(1024)

const (
	VOLUMES   = 1
	DIRECTORY = 2
)

func (s *PCFSServer) NewVolume(arg *[]byte, entry *rpb.LogEntry) []byte {
	volume := &pb.Volume{}
	proto.Unmarshal(*arg, volume)
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
	dbKey := append(ComposeKeyPrefix(VOLUMES), key...)
	rootDirDbKey := append(ComposeKeyPrefix(VOLUMES), key...)
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
