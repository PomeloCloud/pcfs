package storage

import (
	bftraft "github.com/PomeloCloud/BFTRaft4go/server"
	pcfs "github.com/PomeloCloud/pcfs/server"
	"log"
	"os"
	"os/signal"
	"time"
)

var FS PCFS

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

func putExampleFiles(fs *PCFS) {
	fs.PutFile("example.jpg", "example.jpg")
	fs.PutFile("CV.pdf", "CV.pdf")
	log.Println("example files:", fs.Ls(fs.Home()).Items)
	bufferSize := 1024
	stream, err := fs.NewStream(fs.Home() + "/example.jpg")
	fo, err := os.Create("example.out.jpg")
	if err != nil {
		panic(err)
	}
	for true {
		b := make([]byte, bufferSize)
		n, err := stream.Read(&b)
		if err != nil {
			panic(err)
		}
		wb := b[0:n]
		fo.Write(wb)
		if n < uint64(bufferSize) {
			break
		}
	}
	fo.Close()
}

func Main() {
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
	pfs := PCFS{Network: fs}
	pfs.NewVolume()
	time.Sleep(1 * time.Second)
	putExampleFiles(&pfs)
	FS = pfs
	handleExit(fs)
}