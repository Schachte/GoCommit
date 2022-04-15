package config

import (
	"io/ioutil"
	"log"
	"os"
)

func ConfigFile(filename string) (string, string) {
	f, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Panic(err)
	}
	return string(f), filename
}

func LoadFileFromPath(filename string) (*os.File, error) {
	f, err := os.OpenFile(filename, os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	return f, nil
}
