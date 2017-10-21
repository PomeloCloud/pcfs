package main

import (
	"fmt"
	bftraft "github.com/PomeloCloud/BFTRaft4go/server"
	"github.com/PomeloCloud/pcfs/drone/storage"
	pcfs "github.com/PomeloCloud/pcfs/server"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"
)

func initDB(dbPath string) {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dbPath, os.ModePerm); err != nil {
			panic(err)
		}
		log.Println("cannot find wallet, will create one")
		bftraft.InitDatabase(dbPath)
	} else {
		log.Println("wallet already exists")
	}
}

func handleExit(fs *pcfs.PCFSServer) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		log.Println("gracelly shutdown and write back wallet")
		fs.BFTRaft.DB.Close()
		os.Exit(0)
	}()
}

func putExampleFiles(fs *storage.PCFS) {
	stream, err := fs.NewStream(fmt.Sprint("/", strconv.Itoa(int(fs.Network.BFTRaft.Id)), "/example.jpg"))
	if err != nil {
		panic(err)
	}
	f, err := os.Open("example.jpg")
	for true {
		b := make([]byte, 1024)
		n, err := f.Read(b)
		if err != nil {
			panic(err)
		}
		wb := b[0:n]
		stream.Write(&wb)
		if n < 1024 {
			break
		}
	}
	log.Println("inset file succeed")
	stream.Seek(0)
	fo, err := os.Create("example.out.jpg")
	if err != nil {
		panic(err)
	}
	for true {
		b := make([]byte, 1024)
		n, err := stream.Read(&b)
		if err != nil {
			panic(err)
		}
		wb := b[0:n]
		fo.Write(wb)
		if n < 1024 {
			break
		}
	}
	fo.Close()
}

func main() {
	log.Println("PomeloCloud Drone Node")
	log.Println("2017 Shisoft Research and Pomelo Foundation")
	networkConfig := bftraft.ReadConfigFile("network.json")
	initDB(networkConfig.Db)
	log.Print("join network...")
	bftRaft, err := bftraft.GetServer(bftraft.Options{
		DBPath:           networkConfig.Db,
		Address:          networkConfig.Address,
		Bootstrap:        networkConfig.Bootstraps,
		ConsensusTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	time.Sleep(1 * time.Second)
	fs := pcfs.GetServer(bftRaft)
	log.Println("registering storage contracts")
	fs.RegisterStorageContracts()
	bftRaft.StartServer()
	time.Sleep(1 * time.Second)
	fs.CheckJoinAlphaGroup()
	//time.Sleep(1 * time.Second)
	//fs.CheckStashGroup(true)
	fs.RegisterNode(pcfs.ReadConfigFile("storage.json"))
	pfs := storage.PCFS{Network: fs}
	pfs.NewVolume()
	putExampleFiles(&pfs)
	handleExit(fs)
	make(chan bool) <- true
}
