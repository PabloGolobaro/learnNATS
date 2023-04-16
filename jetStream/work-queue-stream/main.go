package main

import (
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"os"
)

func main() {

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, _ := nats.Connect(url)

	defer nc.Drain()

	js, _ := nc.JetStream()

	cfg := &nats.StreamConfig{
		Name:      "EVENTS",
		Retention: nats.WorkQueuePolicy,
		Subjects:  []string{"events.>"},
	}

	js.AddStream(cfg)
	fmt.Println("created the stream")

	js.Publish("events.us.page_loaded", nil)
	js.Publish("events.eu.mouse_clicked", nil)
	js.Publish("events.us.input_focused", nil)
	fmt.Println("published 3 messages")

	fmt.Println("# Stream info without any consumers")
	printStreamState(js, cfg.Name)

	sub1, _ := js.PullSubscribe("", "processor-1", nats.BindStream(cfg.Name))

	msgs, _ := sub1.Fetch(3)
	for _, msg := range msgs {
		msg.AckSync()
	}

	fmt.Println("\n# Stream info with one consumer")
	printStreamState(js, cfg.Name)

	_, err := js.PullSubscribe("", "processor-2", nats.BindStream(cfg.Name))
	fmt.Println("\n# Create an overlapping consumer")
	fmt.Println(err)

	sub1.Unsubscribe()

	sub2, err := js.PullSubscribe("", "processor-2", nats.BindStream(cfg.Name))
	fmt.Printf("created the new consumer? %v\n", err == nil)
	sub2.Unsubscribe()

	fmt.Println("\n# Create non-overlapping consumers")
	sub1, _ = js.PullSubscribe("events.us.>", "processor-us", nats.BindStream(cfg.Name))
	sub2, _ = js.PullSubscribe("events.eu.>", "processor-eu", nats.BindStream(cfg.Name))

	js.Publish("events.eu.mouse_clicked", nil)
	js.Publish("events.us.page_loaded", nil)
	js.Publish("events.us.input_focused", nil)
	js.Publish("events.eu.page_loaded", nil)
	fmt.Println("published 4 messages")

	msgs, _ = sub1.Fetch(2)
	for _, msg := range msgs {
		fmt.Printf("us sub got: %s\n", msg.Subject)
		msg.Ack()
	}

	msgs, _ = sub2.Fetch(2)
	for _, msg := range msgs {
		fmt.Printf("eu sub got: %s\n", msg.Subject)
		msg.Ack()
	}

}

func printStreamState(js nats.JetStreamContext, name string) {
	info, _ := js.StreamInfo(name)
	b, _ := json.MarshalIndent(info.State, "", " ")
	fmt.Println("inspecting stream info")
	fmt.Println(string(b))
}
