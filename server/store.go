package server

import (
	bft "github.com/PomeloCloud/BFTRaft4go/server"
	"github.com/PomeloCloud/BFTRaft4go/utils"
	pb "github.com/PomeloCloud/pcfs/proto"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	"log"
)

func DBKey(group uint64, t uint32, key []byte) []byte {
	return append(bft.ComposeKeyPrefix(group, t), key...)
}

func FileKey(volume []byte, path []byte, index uint64) []byte {
	hash, _ := utils.SHA1Hash(append(append(volume, path...), utils.U64Bytes(index)...))
	return hash
}

func BlockDBKey(group uint64, file []byte, index uint64) []byte {
	// group 3 is only for block storage
	return DBKey(3, BLOCKS, append(append(utils.U64Bytes(group), utils.U64Bytes(index)...), file...))
}

func GetDirectory(txn *badger.Txn, group uint64, key []byte) (*pb.Directory, error) {
	dbkey := DBKey(group, DIRECTORY, key)
	dirItem, err := txn.Get(dbkey)
	if err != nil {
		log.Println("cannot get dir item:", err)
		return nil, err
	}
	dirValue, err := dirItem.Value()
	if err != nil {
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

func SetDirectory(txn *badger.Txn, group uint64, directory *pb.Directory) error {
	dbKey := DBKey(group, DIRECTORY, directory.Key)
	data, err := proto.Marshal(directory)
	if err != nil {
		log.Println("cannot encode dir")
		return err
	}
	return txn.Set(dbKey, data, 0x00)
}

func GetWriteLock(txn *badger.Txn, group uint64, key []byte) (*pb.FileWriteLock, error) {
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

func SetWriteLock(txn *badger.Txn, group uint64, lock *pb.FileWriteLock) error {
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

func GetFile(txn *badger.Txn, group uint64, key []byte) (*pb.FileMeta, error) {
	dbkey := DBKey(group, FILE_META, key)
	fileItem, err := txn.Get(dbkey)
	if err != nil {
		log.Println("cannot get file item:", err)
		return nil, err
	}
	fileValue, err := fileItem.Value()
	if err != nil {
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

func SetFile(txn *badger.Txn, group uint64, file *pb.FileMeta) error {
	dbKey := DBKey(group, FILE_META, file.Key)
	data, err := proto.Marshal(file)
	if err != nil {
		log.Println("cannot encode file")
		return err
	}
	return txn.Set(dbKey, data, 0x00)
}

func GetVolume(txn *badger.Txn, group uint64, key []byte) (*pb.Volume, error) {
	dbkey := DBKey(group, VOLUMES, key)
	volItem, err := txn.Get(dbkey)
	if err != nil {
		log.Println("cannot get vol item:", err)
		return nil, err
	}
	volValue, err := volItem.Value()
	if err != nil {
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

func GetBlockData(txn *badger.Txn, group uint64, file []byte, index uint64) (*pb.BlockData, error) {
	dbkey := BlockDBKey(group, file, index)
	bdItem, err := txn.Get(dbkey)
	if err != nil {
		log.Println("cannot get bd item:", err)
		return nil, err
	}
	bdValue, err := bdItem.Value()
	if err != nil {
		log.Println("cannot get bd value:", err)
		return nil, err
	}
	vol := &pb.BlockData{}
	if err := proto.Unmarshal(bdValue, vol); err != nil {
		log.Println("cannot decode bd:", err)
		return nil, err
	}
	log.Println("got block:", index, vol.Data)
	return vol, nil
}

func SetBlock(txn *badger.Txn, block *pb.BlockData) error {
	dbKey := BlockDBKey(block.Group, block.File, block.Index)
	data, err := proto.Marshal(block)
	log.Println("set block:", block.Index, block.Data)
	if err != nil {
		log.Println("cannot encode block")
		return err
	}
	return txn.Set(dbKey, data, 0x00)
}

func GetHostStash(txn *badger.Txn, group uint64, nodeId uint64) (*pb.HostStash, error) {
	dbkey := DBKey(group, STASH, utils.U64Bytes(nodeId))
	volItem, err := txn.Get(dbkey)
	if err != nil {
		log.Println("cannot get host stash item:", err)
		return nil, err
	}
	hValue, err := volItem.Value()
	if err != nil {
		log.Println("cannot get host stash value:", err)
		return nil, err
	}
	h := &pb.HostStash{}
	if err := proto.Unmarshal(hValue, h); err != nil {
		log.Println("cannot decode host stash:", err)
		return nil, err
	}
	return h, nil
}

func SetHostStash(txn *badger.Txn, group uint64, host *pb.HostStash) error {
	dbKey := DBKey(group, STASH, utils.U64Bytes(host.HostId))
	data, err := proto.Marshal(host)
	if err != nil {
		log.Println("cannot encode host stash")
		return err
	}
	return txn.Set(dbKey, data, 0x00)
}
