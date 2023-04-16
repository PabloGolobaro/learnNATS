package main

import (
	"encoding/json"
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

	js, _ := nc.JetStream()

	cfg := nats.StreamConfig{
		Name:     "EVENTS",
		Subjects: []string{"events.>"},
		Storage:  nats.FileStorage,
	}

	_, err := js.AddStream(&cfg)
	if err != nil {
		logger.Println(err)
		return
	}

	pubAck, err := js.Publish("events.page_loaded", nil)
	if err != nil {
		logger.Println(err)
		return
	}
	fmt.Println(pubAck)
	js.Publish("events.mouse_clicked", nil)
	js.Publish("events.mouse_clicked", nil)
	js.Publish("events.page_loaded", nil)
	js.Publish("events.mouse_clicked", nil)
	js.Publish("events.input_focused", nil)
	fmt.Println("published 6 messages")

	js.PublishAsync("events.input_changed", nil)
	js.PublishAsync("events.input_blurred", nil)
	js.PublishAsync("events.key_pressed", nil)
	js.PublishAsync("events.input_focused", nil)
	js.PublishAsync("events.input_changed", nil)
	js.PublishAsync("events.input_blurred", nil)

	select {
	case <-js.PublishAsyncComplete():
		fmt.Println("published 6 messages")
	case <-time.After(time.Second):
		log.Fatal("publish took too long")
	}

	printStreamState(js, cfg.Name)

	cfg.MaxMsgs = 10
	js.UpdateStream(&cfg)
	fmt.Println("set max messages to 10")

	printStreamState(js, cfg.Name)

	cfg.MaxAge = time.Second
	js.UpdateStream(&cfg)
	fmt.Println("set max age to one second")

	printStreamState(js, cfg.Name)

	<-time.After(time.Second)

	printStreamState(js, cfg.Name)

}

func printStreamState(js nats.JetStreamContext, name string) {
	info, _ := js.StreamInfo(name)
	b, _ := json.MarshalIndent(info.State, "", " ")
	fmt.Println("inspecting stream info")
	fmt.Println(string(b))
}
