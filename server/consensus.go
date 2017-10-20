package server

import (
	"github.com/PomeloCloud/BFTRaft4go/utils"
	pb "github.com/PomeloCloud/pcfs/proto"
	"log"
	"time"
	"github.com/golang/protobuf/proto"
	"context"
	"errors"
)

func GetPeerRPC(addr string) pb.PCFSClient {
	conn, err := utils.GetClientConn(addr)
	if err != nil {
		log.Println("cannot get peer rpc:", err)
		return nil
	}
	return pb.NewPCFSClient(conn)
}

func (s *PCFSServer) GroupMajorityResponse(group uint64, f func(client pb.PCFSClient) (interface{}, []byte)) interface{} {
	hosts := s.BFTRaft.Client.GetGroupHosts(group)
	if hosts == nil {
		msg := "cannot fetch group members"
		log.Println(msg)
		return nil
	}
	clients := []pb.PCFSClient{}
	for _, h := range *hosts {
		c := GetPeerRPC(h.ServerAddr)
		if c != nil {
			clients = append(clients, c)
		}
	}
	ResChan := make(chan utils.FuncResult, len(clients))
	for _, c := range clients {
		if c != nil {
			dataReceived := make(chan utils.FuncResult)
			go func() {
				res, fea := f(c)
				dataReceived <- utils.FuncResult{
					Result:  res,
					Feature: fea,
				}
			}()
			go func() {
				select {
				case res := <-dataReceived:
					ResChan <- res
				case <-time.After(10 * time.Second):
					ResChan <- utils.FuncResult{
						Result:  nil,
						Feature: []byte{},
					}
				}
			}()
		}
	}
	hashes := []uint64{}
	vals := map[uint64]interface{}{}
	for i := 0; i < len(clients); i++ {
		fr := <-ResChan
		if fr.Result == nil {
			continue
		}
		hash := utils.HashData(fr.Feature)
		hashes = append(hashes, hash)
		vals[hash] = fr.Result
	}
	majorityHash := utils.PickMajority(hashes)
	if val, found := vals[majorityHash]; found {
		return val
	} else {
		for _, v := range vals {
			return v
		}
	}
	return nil
}

func (s *PCFSServer)GetMajorityFileMeta(group uint64, file []byte) (*pb.FileMeta, error) {
	fileData := s.GroupMajorityResponse(group, func(client pb.PCFSClient) (interface{}, []byte) {
		file, err := client.GetFileMeta(context.Background(), &pb.GetFileRequest{
			Group: group,
			File: file,
		})
		if err != nil {
			log.Println("cannot get file meta for create block")
			return nil, []byte{}
		} else {
			feature, err := proto.Marshal(file)
			if err != nil {
				log.Println("cannot get feature for file meta for create block")
			}
			return file, feature
		}
	})
	var meta *pb.FileMeta
	if fileData == nil {
		msg := "majority response nil for getting file meta in create block"
		log.Println(msg)
		return nil, errors.New(msg)
	} else {
		meta = fileData.(*pb.FileMeta)
		return meta, nil
	}
}
