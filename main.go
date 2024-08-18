package main

import (
	"gopkg.in/yaml.v3"
	"io"
	"os"
)

func main() {
	confFile := os.Args[1]
	conf, err := os.Open(confFile)
	if err != nil {
		panic(err)
	}
	bConfStr, err := io.ReadAll(conf)
	if err != nil {
		panic(err)
	}
	var config Config
	if err = yaml.Unmarshal(bConfStr, &config); err != nil {
		panic(err)
	}

	generator, err := NewGenerator(config)
	if err != nil {
		panic(err)
	}

	if err = generator.Generate(); err != nil {
		panic(err)
	}
}
