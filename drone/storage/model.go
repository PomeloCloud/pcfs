package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/PomeloCloud/BFTRaft4go/utils"
	pb "github.com/PomeloCloud/pcfs/proto"
	serv "github.com/PomeloCloud/pcfs/server"
	"github.com/golang/protobuf/proto"
	"log"
	"path"
	"strconv"
	"time"
)

type PCFS struct {
	Network *serv.PCFSServer
}

type FileStream struct {
	filesystem        *PCFS
	volume            *pb.Volume
	meta              *pb.FileMeta
	Offset            uint64
	currentBlockData  *pb.BlockData
	currentBlockDirty bool
}

func (fs *PCFS) Ls(dirPath string) *pb.ListDirectoryResponse {
	dirI := fs.Network.GroupMajorityResponse(serv.STASH_GROUP, func(client pb.PCFSClient) (interface{}, []byte) {
		res, err := client.ListDirectory(context.Background(), &pb.ListDirectoryRequest{
			Group: serv.STASH_GROUP,
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
	dir, filename := path.Split(filepath)
	dirRes := fs.Ls(dir)
	if dirRes == nil {
		return nil, errors.New("cannot found dir for stream")
	}
	for _, item := range dirRes.Items {
		if item.Type == pb.DirectoryItem_FILE && item.File.Name == filename {
			return &FileStream{
				meta:              item.File,
				volume:            dirRes.Volume,
				Offset:            0,
				currentBlockData:  nil, // lazy load
				currentBlockDirty: false,
				filesystem:        fs,
			}, nil
		}
	}
	// filename not found, touch it
	if err := fs.touchFile(dirRes.Volume.Key, dirRes.Key, filename); err != nil {
		log.Println("cannot touch file:", err)
		return nil, err
	} else {
		log.Println("file touched, retry:", filepath)
		return fs.NewStream(filepath)
	}
	return nil, errors.New("cannot find filename for stream")
}

func (fs *PCFS) touchFile(volume []byte, dir []byte, filename string) error {
	touchFileContract := &pb.TouchFileContract{
		ClientTime: uint64(time.Now().UnixNano()),
		Name:       filename,
		Dir:        dir,
		Volume:     volume,
	}
	contractData, err := proto.Marshal(touchFileContract)
	if err != nil {
		return err
	}
	res, err := fs.Network.BFTRaft.Client.ExecCommand(serv.STASH_GROUP, serv.TOUCH_FILE, contractData)
	if err != nil {
		return err
	} else {
		if (*res)[0] == 1 {
			return nil
		} else {
			return errors.New("touch file failed")
		}
	}
}

func (fs *FileStream) newBlock(file []byte, index uint64) (*pb.FileMeta, error) {
	log.Println("create block at index:", index)
	hostSuggestionsI := fs.filesystem.Network.GroupMajorityResponse(
		serv.STASH_GROUP,
		func(client pb.PCFSClient) (interface{}, []byte) {
			suggestion, err := client.SuggestBlockStash(context.Background(), &pb.BlockStashSuggestionRequest{
				Group: serv.STASH_GROUP,
				Num:   fs.volume.Replications * 2,
			})
			if err != nil {
				log.Println("cannot create new block", err)
			}
			features, _ := utils.SHA1Hash(HashHostStash(suggestion.Nodes))
			if err == nil {
				return suggestion.Nodes, features
			} else {
				log.Print("cannot get suggestion:", err)
				return nil, []byte{0}
			}
		})
	if hostSuggestionsI == nil {
		msg := fmt.Sprint("cannot get new block suggestion, it's null")
		log.Print(msg)
		return nil, errors.New(msg)
	}
	hostSuggestions := hostSuggestionsI.([]*pb.HostStash)
	succeedReplicas := []uint64{}
	raft := fs.filesystem.Network.BFTRaft
	blockReq := &pb.CreateBlockRequest{
		Group: serv.STASH_GROUP,
		Index: index,
		File:  file,
	}
	for _, host := range hostSuggestions {
		host := raft.GetHostNTXN(host.HostId)
		c := serv.GetPeerRPC(host.ServerAddr)
		if res, err := c.CreateBlock(context.Background(), blockReq); err == nil {
			if res.Succeed {
				succeedReplicas = append(succeedReplicas, host.Id)
			}
		} else {
			log.Println("cannot set block to stash:", err)
		}
	}
	if len(succeedReplicas) == 0 {
		return nil, errors.New("cannot get replica servers")
	}
	commitContract := &pb.CommitBlockContract{
		Index:      index,
		ClientTime: uint64(time.Now().UnixNano()),
		NodeIds:    succeedReplicas,
		File:       file,
	}
	contractData, err := proto.Marshal(commitContract)
	if err != nil {
		msg := fmt.Sprint("cannot encode commit contract:", err)
		log.Print(msg)
		return nil, errors.New(msg)
	}
	res, err := raft.Client.ExecCommand(serv.STASH_GROUP, serv.COMMIT_BLOCK, contractData)
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
		} else {
			msg := fmt.Sprint("commit block contract failed")
			log.Print(msg)
			return nil, errors.New(msg)
		}
	}
}

func (fs *FileStream) getBlock(index uint64) error {
	if fs.currentBlockData != nil && fs.currentBlockData.Index == index {
		log.Println("don't need to get block, it's already there")
		return nil
	}
	fs.LandWrite()
	blockI := fs.filesystem.Network.GroupMajorityResponse(
		serv.STASH_GROUP,
		func(client pb.PCFSClient) (interface{}, []byte) {
			block, err := client.GetBlock(context.Background(), &pb.GetBlockRequest{
				Group: serv.STASH_GROUP,
				Index: index,
				File:  fs.meta.Key,
			})
			if err != nil {
				msg := "cannot get block"
				log.Println(msg)
				return nil, []byte{}
			} else {
				return block, block.Data
			}
		},
	)
	if blockI == nil {
		msg := fmt.Sprint("cannot get block data for: ", index)
		log.Println(msg)
		return errors.New(msg)
	} else {
		log.Println("got block data:", index)
		fs.currentBlockData = blockI.(*pb.BlockData)
		return nil
	}
}

func (fs *FileStream) ensureBlock() error {
	blockSize := uint64(fs.meta.BlockSize)
	var blockIndex uint64 = fs.Offset / blockSize
	if fs.currentBlockData == nil || fs.currentBlockData.Index != blockIndex {
		if len(fs.meta.Blocks) == 0 {
			newMeta, err := fs.newBlock(fs.meta.Key, 0)
			if err != nil {
				log.Println("cannot ensure first block:", err)
				return err
			}
			fs.meta = newMeta
		}
		for i := uint64(len(fs.meta.Blocks)); i <= blockIndex; i++ {
			newMeta, err := fs.newBlock(fs.meta.Key, i)
			if err != nil {
				log.Println("cannot ensure block ", i, ":", err)
				return err
			}
			fs.meta = newMeta
		}
		fs.getBlock(blockIndex)
	}
	return nil
}

func (fs *FileStream) Seek(pos uint64) error {
	fs.Offset = pos
	return fs.ensureBlock()
}

func (fs *FileStream) Read(bytes *[]byte) (uint64, error) {
	if bytes == nil {
		return 0, errors.New("need a sized byte buffer")
	}
	origOffset := fs.Offset
	var i uint64
	for i = 0; i < uint64(len(*bytes)); i++ {
		fs.ensureBlock()
		blockOffset := uint32(origOffset+i) % fs.meta.BlockSize
		if blockOffset > fs.currentBlockData.Tail {
			log.Println("reached tail, read exited")
			break
		}
		(*bytes)[i] = fs.currentBlockData.Data[i]
		if i < uint64(len(*bytes))-1 {
			fs.Offset++
		}
	}
	return i, nil
}

func (fs *FileStream) Write(bytes *[]byte) (uint64, error) {
	if bytes == nil {
		return 0, errors.New("need a sized byte buffer")
	}
	origOffset := fs.Offset
	var i uint64
	for i = 0; i < uint64(len(*bytes)); i++ {
		fs.ensureBlock()
		fs.currentBlockDirty = true
		blockOffset := uint32(origOffset+i) % fs.meta.BlockSize
		if blockOffset > fs.currentBlockData.Tail {
			fs.currentBlockData.Tail++
		}
		fs.currentBlockData.Data[blockOffset] = (*bytes)[i]
		fs.currentBlockDirty = true
		if i < uint64(len(*bytes))-1 {
			fs.Offset++
		}
	}
	return i, nil
}

// write all buffed data into the file system
func (fs *FileStream) LandWrite() {
	if !fs.currentBlockDirty {
		return
	}
	if fs.currentBlockData == nil {
		log.Println("cannot land write, block data is nil")
	}
	index := fs.currentBlockData.Index
	blockMeta := fs.meta.Blocks[index]
	hostIds := blockMeta.Hosts
	for _, hostId := range hostIds {
		host := fs.filesystem.Network.BFTRaft.GetHostNTXN(hostId)
		if host == nil {
			log.Println("cannot find host:", hostId)
		}
		client := serv.GetPeerRPC(host.ServerAddr)
		wr, err := client.SetBlock(context.Background(), fs.currentBlockData)
		if err != nil {
			log.Println("cannot set block to", hostId, err)
		} else {
			if wr.Succeed == true {
				log.Println("set block succeed")
			} else {
				log.Println("set block failed")
			}
		}
	}
	fs.currentBlockDirty = false
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

func (fs *PCFS) NewVolume() {
	vol := &pb.Volume{
		Name:         strconv.Itoa(int(fs.Network.BFTRaft.Id)),
		Key:          []byte{},
		Replications: 3,
		BlockSize:    8 * 1024,
		RootDir:      []byte{},
	}
	volData, err := proto.Marshal(vol)
	if err != nil {
		panic(err)
	}
	volRes, err := fs.Network.BFTRaft.Client.ExecCommand(serv.STASH_GROUP, serv.NEW_VOLUME, volData)
	if err != nil {
		panic(err)
	}
	log.Println("new vol res:", volRes)
}
