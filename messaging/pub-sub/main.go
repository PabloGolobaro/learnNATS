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

	err := nc.Publish("greet.joe", []byte("hello"))
	if err != nil {
		logger.Println(err)
	}

	sub, _ := nc.SubscribeSync("greet.*")

	msg, err := sub.NextMsg(10 * time.Millisecond)
	if err != nil {
		logger.Println(err)
	}
	fmt.Println("subscribed after a publish...")
	fmt.Printf("msg is nil? %v\n", msg == nil)

	err = nc.Publish("greet.joe", []byte("hello"))
	if err != nil {
		logger.Println(err)
	}

	err = nc.Publish("greet.pam", []byte("hello"))
	if err != nil {
		logger.Println(err)
	}

	msg, _ = sub.NextMsg(10 * time.Millisecond)
	fmt.Printf("msg data: %q on subject %q\n", string(msg.Data), msg.Subject)

	msg, _ = sub.NextMsg(10 * time.Millisecond)
	fmt.Printf("msg data: %q on subject %q\n", string(msg.Data), msg.Subject)

	err = nc.Publish("greet.bob", []byte("hello"))
	if err != nil {
		logger.Println(err)
	}

	msg, _ = sub.NextMsg(10 * time.Millisecond)
	fmt.Printf("msg data: %q on subject %q\n", string(msg.Data), msg.Subject)
}
