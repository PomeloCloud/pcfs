package server

import (
	bft "github.com/PomeloCloud/BFTRaft4go/server"
	pb "github.com/PomeloCloud/pcfs/proto"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	"log"
)

func DirDBKey(group uint64, key []byte) []byte {
	return append(bft.ComposeKeyPrefix(group, DIRECTORY), key...)
}

func (s *PCFSServer) GetDirectory(txn *badger.Txn, group uint64, key []byte) (*pb.Directory, error) {
	dbkey := DirDBKey(group, key)
	dirItem, err := txn.Get(dbkey)
	if err == nil {
		log.Println("cannot get dir item:", err)
		return nil, err
	}
	parentDirValue, err := dirItem.Value()
	if err == nil {
		log.Println("cannot get dir value:", err)
		return nil, err
	}
	dir := &pb.Directory{}
	if err := proto.Unmarshal(parentDirValue, dir); err != nil {
		log.Println("cannot decode dir:", err)
		return nil, err
	}
	return dir, nil
}

func (s *PCFSServer) SetDirectory(txn *badger.Txn, group uint64, directory *pb.Directory) error {
	dbKey := DirDBKey(group, directory.Key)
	data, err := proto.Marshal(directory)
	if err != nil {
		log.Println("cannot encode dir")
		return err
	}
	return txn.Set(dbKey, data, 0x00)
}
