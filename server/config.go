package storage

import (
	"io/ioutil"
	"log"
	"encoding/json"
)

type FileConfig struct {
	Capacity string
}

func ReadConfigFile(path string) FileConfig {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	log.Println(string(data))
	fc := FileConfig{}
	if err := json.Unmarshal(data, &fc); err != nil {
		panic(err)
	}
	return fc
}
