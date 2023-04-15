package main

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"log"
	"os"
	"sync"
	"time"
)

func main() {
	var wg sync.WaitGroup

	logger := log.New(os.Stdout, "", log.Lshortfile)

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, _ := nats.Connect(url)

	defer nc.Drain()

	msgChan := make(chan *nats.Msg, 10)

	chanSubscribe, err := nc.ChanSubscribe("greet.joe", msgChan)
	if err != nil {
		logger.Println(err)
	}

	wg.Add(50)

	for i := 0; i < 10; i++ {
		go func(num int) {
			for msg := range msgChan {
				fmt.Printf("Got message:(%s) in goroutine %d\n", string(msg.Data), num)
				wg.Done()
			}
		}(i)
	}

	for i := 0; i < 50; i++ {
		msg := fmt.Sprintf("Hello %d", i)
		err := nc.Publish("greet.joe", []byte(msg))
		if err != nil {
			logger.Println(err)
		}
		<-time.After(time.Nanosecond * 1)
	}

	logger.Println("Closing")
	wg.Wait()

	err = chanSubscribe.Unsubscribe()
	if err != nil {
		logger.Println(err)
	}
	close(msgChan)

}
