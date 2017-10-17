package client

import pb "github.com/PomeloCloud/pcfs/proto"

type PCFS struct {
	volume *pb.Volume
}

type FileStream struct {
	meta *pb.FileMeta
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
