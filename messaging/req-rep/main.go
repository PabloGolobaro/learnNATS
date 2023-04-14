package main

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"log"
	"os"
	"time"
)

func main() {
	logger := log.New(os.Stdout, "", log.Lshortfile)

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, _ := nats.Connect(url)
	defer nc.Drain()

	sub, _ := nc.Subscribe("greet.*", func(msg *nats.Msg) {
		name := msg.Subject[6:]
		err := msg.Respond([]byte("hello, " + name))
		if err != nil {
			logger.Println(err)
		}
	})

	rep, err := nc.Request("greet.joe", nil, time.Second)
	if err != nil {
		logger.Println(err)
	}
	fmt.Println(string(rep.Data))

	rep, _ = nc.Request("greet.sue", nil, time.Second)
	fmt.Println(string(rep.Data))

	rep, _ = nc.Request("greet.bob", nil, time.Second)
	fmt.Println(string(rep.Data))

	sub.Unsubscribe()

	_, err = nc.Request("greet.joe", nil, time.Second)
	if err != nil {
		logger.Println(err)
	}
}
