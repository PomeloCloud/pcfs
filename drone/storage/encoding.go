package storage

import (
	pb "github.com/PomeloCloud/pcfs/proto"
	"github.com/golang/protobuf/proto"
)

func HashHostStash(hosts []*pb.HostStash) []byte {
	hostData := []byte{}
	for _, host := range hosts {
		data, err := proto.Marshal(host)
		if err != nil {
			panic(err)
		} else {
			hostData = append(hostData, data...)
		}
	}
	return hostData
}
