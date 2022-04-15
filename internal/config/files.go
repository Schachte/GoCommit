package config

import (
	"io/ioutil"
	"log"
)

func ConfigFile(filename string) (string, string) {
	f, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Panic(err)
	}

	return string(f), filename
}
