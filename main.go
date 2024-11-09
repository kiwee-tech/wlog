package main

import (
	"errors"
	"fmt"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/joho/godotenv"
	"log"
	"os"
	"time"
)

func main() {
	argsWithProg := os.Args
	if len(argsWithProg) < 2 {
		log.Fatal("Argument missing.")
	}

	loadEnvVariables()
	beanstalkDsn := os.Getenv("BEANSTALK_DSN")

	t := argsWithProg[1]
	if t == "consumer" {
		consumer(beanstalkDsn)
	} else if t == "producer" {
		producer(beanstalkDsn)
	} else {
		fmt.Println("Invalid argument.")
	}
}

func consumer(beanstalkDsn string) {
	conn, _ := connect(beanstalkDsn)

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

func producer(beanstalkDsn string) {
	conn, _ := connect(beanstalkDsn)

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

func connect(beanstalkDsn string) (*beanstalk.Conn, error) {
	conn, err := beanstalk.Dial("tcp", beanstalkDsn)
	if err != nil {
		log.Fatal(err)
	}

	return conn, err
}

func loadEnvVariables() {
	err := godotenv.Load(".env")

	if err != nil {
		log.Fatal("Error loading .env file")
	}
}
