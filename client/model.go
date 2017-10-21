package client

import (
	"context"
	"errors"
	"github.com/PomeloCloud/BFTRaft4go/utils"
	pb "github.com/PomeloCloud/pcfs/proto"
	. "github.com/PomeloCloud/pcfs/server"
	"github.com/golang/protobuf/proto"
	"log"
	"path"
	"fmt"
	"time"
)

type PCFS struct {
	network *PCFSServer
}

type FileStream struct {
	filesystem         *PCFS
	volume             *pb.Volume
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
				meta:               item.File,
				volume:             dirRes.Volume,
				currentBlockIndex:  0,
				currentBlockOffset: 0,
				currentBlockData:   nil, // lazy load
			}, nil
		}
	}
	return nil, errors.New("cannot find file for stream")
}

func (fs *FileStream) newBlock(file []byte, index uint64) (*pb.FileMeta, error) {
	hostSuggestionsI := fs.filesystem.network.GroupMajorityResponse(
		STASH_REG,
		func(client pb.PCFSClient) (interface{}, []byte) {
			suggestion, err := client.SuggestBlockStash(context.Background(), &pb.BlockStashSuggestionRequest{
				Group: STASH_REG,
				Num:   fs.volume.Replications * 2,
			})
			features, _ := utils.SHA1Hash(HashHostStash(suggestion.Nodes))
			if err == nil {
				return suggestion.Nodes, features
			} else {
				log.Print("cannot get suggestion:", err)
				return nil, []byte{0}
			}
		})
	hostSuggestions := []*pb.HostStash{}
	if hostSuggestionsI == nil {
		msg := fmt.Sprint("cannot get new block suggestion, it's null")
		log.Print(msg)
		return nil, errors.New(msg)
	}
	hostSuggestions = hostSuggestionsI.([]*pb.HostStash{})
	succeedReplicas := []uint64{}
	raft := fs.filesystem.network.BFTRaft
	for _, host := range hostSuggestions {
		host := raft.GetHostNTXN(host.HostId)
		c := GetPeerRPC(host.ServerAddr)
		if res, err := c.CreateBlock(context.Background(), &pb.CreateBlockRequest{
			Group: STASH_REG,
			Index: index,
			File: file,
		}); err == nil {
			if res.Succeed {
				succeedReplicas = append(succeedReplicas, host.Id)
			}
		}
	}
	if len(succeedReplicas) == 0 {
		return nil, errors.New("cannot get replica servers")
	}
	commitContract := &pb.CommitBlockContract{
		Index: index,
		ClientTime: uint64(time.Now().UnixNano()),
		NodeIds: succeedReplicas,
		File: file,
	}
	contractData, err := proto.Marshal(commitContract)
	if err != nil {
		msg := fmt.Sprint("cannot encode commit contract:", err)
		log.Print(msg)
		return nil, errors.New(msg)
	}
	res, err := raft.Client.ExecCommand(STASH_REG, COMMIT_BLOCK, contractData)
	if err != nil {
		msg := fmt.Sprint("cannot commit block contract", err)
		log.Print(msg)
		return nil, errors.New(msg)
	} else {
		if len(*res) > 1 {
			newMeta := pb.FileMeta{}
			if err := proto.Unmarshal(*res, &newMeta); err == nil {
				return &newMeta, nil
			} else {
				msg := fmt.Sprint("cannot decode new meta", err)
				log.Print(msg)
				return nil, errors.New(msg)
			}
		}
	}
}

func (fs *FileStream) Seek(pos uint64) (uint64, error) {
	blockSize := uint64(fs.meta.BlockSize)
	var blockIndex uint64 = pos / blockSize
	blockOffset := pos % blockSize

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
