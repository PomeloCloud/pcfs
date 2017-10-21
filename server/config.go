package server

import (
	"encoding/json"
	"io/ioutil"
)

type FileConfig struct {
	Capacity string
}

func ReadConfigFile(path string) FileConfig {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	fc := FileConfig{}
	if err := json.Unmarshal(data, &fc); err != nil {
		panic(err)
	}
	return fc
}
