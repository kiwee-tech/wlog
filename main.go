package main

import (
	"errors"
	"fmt"
	"github.com/beanstalkd/go-beanstalk"
	"log"
	"os"
	"time"
)

func main() {
	argsWithProg := os.Args
	if len(argsWithProg) < 2 {
		log.Fatal("Argument missing.")
	}

	t := argsWithProg[1]
	if t == "consumer" {
		consumer()
	} else if t == "producer" {
		producer()
	} else {
		fmt.Println("Invalid argument.")
	}
}

func consumer() {
	conn, _ := connect()

	tubeSet := beanstalk.NewTubeSet(conn, "test")

	for {
		id, body, err := tubeSet.Reserve(5 * time.Second)
		if errors.Is(err, beanstalk.ErrTimeout) {
			log.Printf("timeout\n")
			continue
		}

		if err != nil {
			log.Fatal(err)
		}

		log.Printf("id:%d, body:%s\n", id, string(body))
		err = conn.Delete(id)
		if err != nil {
			log.Fatal(err)
		}
	}

	//if err = conn.Close(); err != nil {
	//	log.Fatal(err)
	//}
}

func producer() {
	conn, _ := connect()

	tube := beanstalk.NewTube(conn, "test")

	id, err := tube.Put([]byte("hello"), 1, 0, 120*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	if err = conn.Close(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Job id %d inserted\n", id)
}

func connect() (*beanstalk.Conn, error) {
	conn, err := beanstalk.Dial("tcp", "127.0.0.1:11300")
	if err != nil {
		log.Fatal(err)
	}

	return conn, err
}
