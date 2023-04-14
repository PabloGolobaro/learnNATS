package main

import (
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"log"
	"os"
	"time"
)

type Payload struct {
	ServiceID int    `json:"service_id"`
	Time      string `json:"time"`
	Message   string `json:"message"`
}

func main() {
	logger := log.New(os.Stdout, "", log.Lshortfile)

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, _ := nats.Connect(url)

	defer nc.Drain()

	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		logger.Println(err)
	}

	payload := Payload{
		ServiceID: 1,
		Time:      time.Now().Format(time.TimeOnly),
		Message:   "Hello gopher",
	}

	bytes, err := json.Marshal(payload)
	if err != nil {
		logger.Println(err)
	}

	err = nc.Publish("foo", bytes)
	if err != nil {
		logger.Println(err)
	}

	msg, err := sub.NextMsg(time.Millisecond * 10)
	if err != nil {
		logger.Println(err)
	}

	payloadMessage := Payload{}
	err = json.Unmarshal(msg.Data, &payloadMessage)
	if err != nil {
		logger.Println(err)
	}

	fmt.Println(payloadMessage)

}
