package client

import (
	pb "github.com/PomeloCloud/pcfs/proto"
	. "github.com/PomeloCloud/pcfs/server"
	"context"
	"log"
	"github.com/golang/protobuf/proto"
)

type PCFS struct {
	network *PCFSServer
	volume *pb.Volume
}

type FileStream struct {
	meta         *pb.FileMeta
	blockSize    uint64
	currentBlock *pb.BlockData
}

func (fs *PCFS) Ls(dirPath string) *pb.ListDirectoryResponse {
	dirI := fs.network.GroupMajorityResponse(STASH_REG, func(client pb.PCFSClient) (interface{}, []byte) {
		res, err := client.ListDirectory(context.Background(), &pb.ListDirectoryRequest{
			Group: STASH_REG,
			Path: dirPath,
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

func (fs *PCFS) NewStream(path string) *FileStream {

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
