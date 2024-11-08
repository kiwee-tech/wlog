package main

import (
	"errors"
	"github.com/beanstalkd/go-beanstalk"
	"log"
	"time"
)

func main() {
	conn, err := beanstalk.Dial("tcp", "127.0.0.1:11300")
	if err != nil {
		log.Fatal(err)
	}

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
