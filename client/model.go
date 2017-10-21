package client

import (
	"context"
	"errors"
	pb "github.com/PomeloCloud/pcfs/proto"
	. "github.com/PomeloCloud/pcfs/server"
	"github.com/golang/protobuf/proto"
	"log"
	"path"
)

type PCFS struct {
	network *PCFSServer
	volume  *pb.Volume
}

type FileStream struct {
	meta               *pb.FileMeta
	currentBlockData   *pb.BlockData
	currentBlockOffset uint64
	currentBlockIndex  int
}

func (fs *PCFS) Ls(dirPath string) *pb.ListDirectoryResponse {
	dirI := fs.network.GroupMajorityResponse(STASH_REG, func(client pb.PCFSClient) (interface{}, []byte) {
		res, err := client.ListDirectory(context.Background(), &pb.ListDirectoryRequest{
			Group: STASH_REG,
			Path:  dirPath,
		})
		if err != nil {
			log.Print("cannot access node for dir list")
			return nil, []byte{}
		} else {
			feature, err := proto.Marshal(res)
			if err != nil {
				log.Print("cannot encode dir list for feature")
				return nil, []byte{}
			}
			return res, feature
		}
	})
	if dirI != nil {
		return dirI.(*pb.ListDirectoryResponse)
	} else {
		log.Print("cannot create new stream")
		return nil
	}
}

func (fs *PCFS) NewStream(filepath string) (*FileStream, error) {
	dir, file := path.Split(filepath)
	dirRes := fs.Ls(dir)
	if dirRes == nil {
		return nil, errors.New("cannot found dir for stream")
	}
	for _, item := range dirRes.Items {
		if item.Type == pb.DirectoryItem_FILE && item.File.Name == file {
			return &FileStream{
				meta: item.File,
				currentBlockIndex: 0,
				currentBlockOffset: 0,
				currentBlockData: nil, // lazy load
			}, nil
		}
	}
	return nil, errors.New("cannot find file for stream")
}

func (fs *FileStream) Seek(pos uint64) uint64 {
	return 0
}

func (fs *FileStream) Read(bytes *[]byte, count uint64) uint64 {
	return 0
}

func (fs *FileStream) Write(bytes *[]byte) uint64 {
	return 0
}

// write all buffed data into the file system
func (fs *FileStream) TouchDown() {

}

func (fs *PCFS) Open(path string) (*FileStream, error) {
	return nil, nil
}

func (fs *PCFS) Mkdir(path string) error {
	return nil
}

func (fs *PCFS) Rm(path string) error {
	return nil
}

func (fs *PCFS) Rmr(path string) error {
	return nil
}

func (fs *PCFS) Mv(path string) error {
	return nil
}
