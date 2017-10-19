package server

import (
	bft "github.com/PomeloCloud/BFTRaft4go/server"
	pb "github.com/PomeloCloud/pcfs/proto"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	"log"
	"github.com/PomeloCloud/BFTRaft4go/utils"
)

func DBKey(group uint64, t uint32, key []byte) []byte {
	return append(bft.ComposeKeyPrefix(group, t), key...)
}

func FileKey(volume []byte, path []byte, index uint64) []byte {
	hash, _ := utils.SHA1Hash(append(append(volume, path...), utils.U64Bytes(index)...))
	return hash
}

func (s *PCFSServer) GetDirectory(txn *badger.Txn, group uint64, key []byte) (*pb.Directory, error) {
	dbkey := DBKey(group, DIRECTORY, key)
	dirItem, err := txn.Get(dbkey)
	if err == nil {
		log.Println("cannot get dir item:", err)
		return nil, err
	}
	dirValue, err := dirItem.Value()
	if err == nil {
		log.Println("cannot get dir value:", err)
		return nil, err
	}
	dir := &pb.Directory{}
	if err := proto.Unmarshal(dirValue, dir); err != nil {
		log.Println("cannot decode dir:", err)
		return nil, err
	}
	return dir, nil
}

func (s *PCFSServer) SetDirectory(txn *badger.Txn, group uint64, directory *pb.Directory) error {
	dbKey := DBKey(group, DIRECTORY, directory.Key)
	data, err := proto.Marshal(directory)
	if err != nil {
		log.Println("cannot encode dir")
		return err
	}
	return txn.Set(dbKey, data, 0x00)
}

func (s *PCFSServer) GetWriteLock(txn *badger.Txn, group uint64, key []byte) (*pb.FileWriteLock, error) {
	dbkey := DBKey(group, FILE_LOCK, key)
	lockItem, err := txn.Get(dbkey)
	if err == nil {
		log.Println("cannot get file lock item:", err)
		return nil, err
	}
	lockValue, err := lockItem.Value()
	if err == nil {
		log.Println("cannot get file lock value:", err)
		return nil, err
	}
	lock := &pb.FileWriteLock{}
	if err := proto.Unmarshal(lockValue, lock); err != nil {
		log.Println("cannot decode file lock:", err)
		return nil, err
	}
	return lock, nil
}

func (s *PCFSServer) SetWriteLock(txn *badger.Txn, group uint64, lock *pb.FileWriteLock) error {
	dbKey := DBKey(group, FILE_LOCK, lock.Key)
	data, err := proto.Marshal(lock)
	if err != nil {
		log.Println("cannot encode lock")
		return err
	}
	return txn.Set(dbKey, data, 0x00)
}

func (s *PCFSServer) ReleaseWriteLock(txn *badger.Txn, group uint64, lock *pb.FileWriteLock) error {
	dbKey := DBKey(group, FILE_LOCK, lock.Key)
	return txn.Delete(dbKey)
}

func (s *PCFSServer) GetFile(txn *badger.Txn, group uint64, key []byte) (*pb.FileMeta, error) {
	dbkey := DBKey(group, FILE_META, key)
	fileItem, err := txn.Get(dbkey)
	if err == nil {
		log.Println("cannot get file item:", err)
		return nil, err
	}
	fileValue, err := fileItem.Value()
	if err == nil {
		log.Println("cannot get file value:", err)
		return nil, err
	}
	file := &pb.FileMeta{}
	if err := proto.Unmarshal(fileValue, file); err != nil {
		log.Println("cannot decode file file:", err)
		return nil, err
	}
	return file, nil
}

func (s *PCFSServer) SetFile(txn *badger.Txn, group uint64, file *pb.FileMeta) error {
	dbKey := DBKey(group, FILE_META, file.Key)
	data, err := proto.Marshal(file)
	if err != nil {
		log.Println("cannot encode file")
		return err
	}
	return txn.Set(dbKey, data, 0x00)
}

func (s *PCFSServer) GetVolume(txn *badger.Txn, group uint64, key []byte) (*pb.Volume, error) {
	dbkey := DBKey(group, VOLUMES, key)
	volItem, err := txn.Get(dbkey)
	if err == nil {
		log.Println("cannot get vol item:", err)
		return nil, err
	}
	volValue, err := volItem.Value()
	if err == nil {
		log.Println("cannot get vol value:", err)
		return nil, err
	}
	vol := &pb.Volume{}
	if err := proto.Unmarshal(volValue, vol); err != nil {
		log.Println("cannot decode vol:", err)
		return nil, err
	}
	return vol, nil
}