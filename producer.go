package main

import (
	"github.com/beanstalkd/go-beanstalk"
	"log"
	"time"
)

func main() {
	conn, err := beanstalk.Dial("tcp", "127.0.0.1:11300")
	if err != nil {
		log.Fatal(err)
	}

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
