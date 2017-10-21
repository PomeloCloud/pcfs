package main

import (
	bftraft "github.com/PomeloCloud/BFTRaft4go/server"
	pcfs "github.com/PomeloCloud/pcfs/server"
	"log"
	"os"
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
	fs.CheckStashGroup(true)
	fs.CheckJoinAlphaGroup()
	fs.RegisterNode(pcfs.ReadConfigFile("storage.json"))
}
