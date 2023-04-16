package main

import (
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"os"
	"time"
)

func main() {

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, _ := nats.Connect(url)

	defer nc.Drain()

	js, _ := nc.JetStream()

	js.AddStream(&nats.StreamConfig{
		Name:     "EVENTS-EU",
		Subjects: []string{"events.eu.>"},
	})

	js.AddStream(&nats.StreamConfig{
		Name:     "EVENTS-US",
		Subjects: []string{"events.us.>"},
	})

	js.AddConsumer("EVENTS-EU", &nats.ConsumerConfig{
		Durable:        "processor",
		DeliverSubject: "push.events",
		DeliverGroup:   "processor",
		AckPolicy:      nats.AckExplicitPolicy,
	})

	js.AddConsumer("EVENTS-US", &nats.ConsumerConfig{
		Durable:        "processor",
		DeliverSubject: "push.events",
		DeliverGroup:   "processor",
		AckPolicy:      nats.AckExplicitPolicy,
	})

	js.Publish("events.eu.page_loaded", nil)
	js.Publish("events.eu.input_focused", nil)
	js.Publish("events.us.page_loaded", nil)
	js.Publish("events.us.mouse_clicked", nil)
	js.Publish("events.eu.mouse_clicked", nil)
	js.Publish("events.us.input_focused", nil)

	sub, _ := nc.QueueSubscribeSync("push.events", "processor")
	defer sub.Drain()

	for {
		msg, err := sub.NextMsg(time.Second)
		if err == nats.ErrTimeout {
			break
		}

		fmt.Println(msg.Subject)
		msg.Ack()
	}

	info1, _ := js.ConsumerInfo("EVENTS-EU", "processor")
	fmt.Printf("eu: last delivered: %d, num pending: %d\n", info1.Delivered.Stream, info1.NumPending)
	info2, _ := js.ConsumerInfo("EVENTS-US", "processor")
	fmt.Printf("us: last delivered: %d, num pending: %d\n", info2.Delivered.Stream, info2.NumPending)
}

func printStreamState(js nats.JetStreamContext, name string) {
	info, _ := js.StreamInfo(name)
	b, _ := json.MarshalIndent(info.State, "", " ")
	fmt.Println("inspecting stream info")
	fmt.Println(string(b))
}
