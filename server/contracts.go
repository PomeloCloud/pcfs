package server

import (
	"bytes"
	"errors"
	rpb "github.com/PomeloCloud/BFTRaft4go/proto/server"
	bft "github.com/PomeloCloud/BFTRaft4go/server"
	"github.com/PomeloCloud/BFTRaft4go/utils"
	pb "github.com/PomeloCloud/pcfs/proto"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	"log"
	"fmt"
	"github.com/patrickmn/go-cache"
)

// Beta group contracts

const _10MB = uint32(10 * 1024 * 1024)
const _1KB = uint32(1024)

const (
	VOLUMES   = 1
	DIRECTORY = 2
	FILE_LOCK = 3
	FILE_META = 4
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
	dbKey := DBKey(group, VOLUMES, key)
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
	contract := &pb.NewDirectoryContract{}
	if err := proto.Unmarshal(*arg, contract); err != nil {
		log.Println("cannot decode new dir contract:", err)
		return []byte{0}
	}
	dir := contract.Dir
	dir.Key, _ = utils.SHA1Hash(append(contract.ParentDir, entry.Hash...))
	dir.Files = [][]byte{}
	newDirToken := append([]byte{1}, dir.Key...)
	if err := s.BFTRaft.DB.Update(func(txn *badger.Txn) error {
		parentDir, err := s.GetDirectory(txn, group, contract.ParentDir)
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

func (s *PCFSServer) AcquireFileWriteLock(arg *[]byte, entry *rpb.LogEntry) []byte {
	group := entry.Command.Group
	contract := &pb.AcquireFileWriteLockContract{}
	if err := proto.Unmarshal(*arg, contract); err != nil {
		log.Println("cannode decode lock contract:", err)
		return []byte{0}
	}
	newLock := &pb.FileWriteLock{
		Group: group, Key: contract.Key,
	}
	if err := s.BFTRaft.DB.Update(func(txn *badger.Txn) error {
		if _, err := s.GetFile(txn, group, contract.Key); err != nil {
			return err
		}
		if lock, err := s.GetWriteLock(txn, group, contract.Key); err == badger.ErrKeyNotFound {
			s.SetWriteLock(txn, group, newLock)
		} else if err == nil {
			if lock.Owner != entry.Command.ClientId {
				return errors.New("lock already acquired")
			} else {
				s.SetWriteLock(txn, group, newLock)
			}
		} else {
			return err
		}
		return nil
	}); err == nil {
		log.Println("file lock acuqired")
		return []byte{1}
	} else {
		log.Println("cannot acquire file lock:", err)
		return []byte{0}
	}
}

func (s *PCFSServer) ReleaseFileWriteLock(arg *[]byte, entry *rpb.LogEntry) []byte {
	group := entry.Command.Group
	contract := &pb.ReleaseFileWriteLockContract{}
	if err := proto.Unmarshal(*arg, contract); err != nil {
		log.Println("cannode decode lock re contract:", err)
		return []byte{0}
	}
	if err := s.BFTRaft.DB.Update(func(txn *badger.Txn) error {
		if lock, err := s.GetWriteLock(txn, group, contract.Key); err == badger.ErrKeyNotFound {
			return errors.New("cannot find the lock")
		} else if err == nil {
			return s.ReleaseWriteLock(txn, group, lock)
		} else {
			return err
		}
		return nil
	}); err == nil {
		log.Println("lock released")
		return []byte{1}
	} else {
		log.Println("cannot release lock:", err)
		return []byte{0}
	}
}

func (s *PCFSServer) TouchFile(arg *[]byte, entry *rpb.LogEntry) []byte {
	group := entry.Command.Group
	contract := &pb.TouchFileContract{}
	if err := proto.Unmarshal(*arg, contract); err != nil {
		log.Println("cannode decode touch file contract:", err)
		return []byte{0}
	}
	fileKey := FileKey(contract.Volume, contract.Dir, entry.Index)
	file := &pb.FileMeta{
		Name:         contract.Name,
		Size:         0,
		LastModified: contract.ClientTime,
		CreatedAt:    contract.ClientTime,
		Key:          fileKey,
		Blocks:       []*pb.Block{},
	}
	if err := s.BFTRaft.DB.Update(func(txn *badger.Txn) error {
		if vol, err := s.GetVolume(txn, group, contract.Volume); err == nil {
			file.BlockSize = vol.BlockSize
		} else {
			return errors.New("cannot find volume for touch file")
		}
		if _, err := s.GetFile(txn, group, fileKey); err == badger.ErrKeyNotFound {
			s.SetFile(txn, group, file)
		}
		return nil
	}); err == nil {
		log.Println("touch file succeed")
		return []byte{1}
	} else {
		log.Println("cannot touch file", err)
		return []byte{0}
	}
}

// Only block created by a signed client message can be confirmed and marked on the ledger
// Clients can pickup any servers it wanted by consulting beta group for host stash space remained
// Setback: client cannot verify whether the data is modified, only pick the majority
// Setback: size of the file can only be calculated by multiply it's block count and block size
// invoked when new block created on storage servers
// TODO: find a way to verify that stash nodes really occupied those spaces
func (s *PCFSServer) ConfirmBlock(arg *[]byte, entry *rpb.LogEntry) []byte {
	group := entry.Command.Group
	contract := &pb.ConfirmBlockContract{}
	if err := proto.Unmarshal(*arg, contract); err != nil {
		log.Println("cannode decode confirm block contract:", err)
		return []byte{0}
	}
	// TODO: verify client signature
	cacheKey := fmt.Sprint(group, "-", contract.Index, "-", contract.File)
	logI, cached := s.PendingBlocks.Get(cacheKey)
	if !cached {
		logI = &map[uint64]bool{contract.NodeId: true}
		s.PendingBlocks.Set(cacheKey, logI, cache.DefaultExpiration)
	} else {
		l := logI.(*map[uint64]bool)
		(*l)[contract.NodeId] = true
		s.PendingBlocks.Set(cacheKey, l, cache.DefaultExpiration)
	}
	return []byte{1}
}
// invoked by client to commit confirmed block that will put into file meta data
func (s *PCFSServer) CommitBlockCreation(arg *[]byte, entry *rpb.LogEntry) []byte {
	group := entry.Command.Group
	contract := &pb.ConfirmBlockCreationContract{}
	if err := proto.Unmarshal(*arg, contract); err != nil {
		log.Println("cannode decode confirm block creation contract:", err)
		return []byte{0}
	}
	cacheKey := fmt.Sprint(group, "-", contract.Index, "-", contract.File)
	logI, cached := s.PendingBlocks.Get(cacheKey)
	if !cached {
		log.Println("cannot found the block for commit")
		return []byte{0}
	}
	l := logI.(*map[uint64]bool)
	hosts := []uint64{}
	// check all replication nodes confirmed
	for _, nodeId := range contract.NodeIds {
		_, confirmed := (*l)[nodeId]
		if !confirmed {
			log.Println("not all replication confirmed:", nodeId)
			return []byte{0}
		} else {
			hosts = append(hosts, nodeId)
		}
	}
	// remove cached
	s.PendingBlocks.Delete(cacheKey)
	// update file meta
	newBlock := &pb.Block{Hosts: hosts}
	if err := s.BFTRaft.DB.Update(func(txn *badger.Txn) error {
		if file, err := s.GetFile(txn, group, contract.File); err != nil {
			blocks := len(file.Blocks)
			if uint64(blocks) != contract.Index {
				return errors.New("new block index not match next index")
			}
			file.Blocks = append(file.Blocks, newBlock)
			file.LastModified = contract.ClientTime
			file.Size = uint64(len(file.Blocks)) * uint64(file.BlockSize)
			s.SetFile(txn, group, file)
		} else {
			return err
		}
		return nil
	}); err == nil {
		log.Println("confirmed new block succeed")
		return []byte{1}
	} else {
		log.Println("failed to confirm new block")
		return []byte{0}
	}
}