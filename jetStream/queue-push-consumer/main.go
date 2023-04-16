package main

import (
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"os"
	"sync"
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

	streamName := "EVENTS"

	cfg := &nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{"events.>"},
	}

	js.AddStream(cfg)

	fmt.Println("# Durable (implicit)")
	sub1, _ := js.QueueSubscribeSync("events.>", "event-processor", nats.AckExplicit())

	info, _ := js.ConsumerInfo(streamName, "event-processor")
	fmt.Printf("deliver group: %q\n", info.Config.DeliverGroup)

	sub2, _ := js.QueueSubscribeSync("events.>", "event-processor", nats.AckExplicit())

	sub3, _ := nc.QueueSubscribeSync(info.Config.DeliverSubject, info.Config.DeliverGroup)
	fmt.Printf("deliver subject: %q\n", info.Config.DeliverSubject)

	js.Publish("events.1", nil)
	js.Publish("events.2", nil)
	js.Publish("events.3", nil)
	js.Publish("events.4", nil)
	js.Publish("events.5", nil)
	js.Publish("events.6", nil)

	msg, _ := sub1.NextMsg(time.Second)
	if msg != nil {
		fmt.Printf("sub1: received message %q\n", msg.Subject)
		msg.Ack()
	} else {
		fmt.Println("sub1: receive timeout")
	}

	msg, _ = sub2.NextMsg(time.Second)
	if msg != nil {
		fmt.Printf("sub2: received message %q\n", msg.Subject)
		msg.Ack()
	} else {
		fmt.Println("sub2: receive timeout")
	}

	msg, _ = sub3.NextMsg(time.Second)
	if msg != nil {
		fmt.Printf("sub3: received message %q\n", msg.Subject)
		msg.Ack()
	} else {
		fmt.Println("sub3: receive timeout")
	}

	sub1.Unsubscribe()
	sub2.Unsubscribe()
	sub3.Unsubscribe()

	fmt.Println("\n# Durable (explicit)")

	js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:        "event-processor",
		DeliverSubject: "my-subject",
		DeliverGroup:   "event-processor",
		AckPolicy:      nats.AckExplicitPolicy,
	})

	defer js.DeleteConsumer(streamName, "event-processor")

	wg := &sync.WaitGroup{}
	wg.Add(6)

	sub1, _ = nc.QueueSubscribe("my-subject", "event-processor", func(msg *nats.Msg) {
		fmt.Printf("sub1: received message %q\n", msg.Subject)
		msg.Ack()
		wg.Done()
	})
	sub2, _ = nc.QueueSubscribe("my-subject", "event-processor", func(msg *nats.Msg) {
		fmt.Printf("sub2: received message %q\n", msg.Subject)
		msg.Ack()
		wg.Done()
	})
	sub3, _ = nc.QueueSubscribe("my-subject", "event-processor", func(msg *nats.Msg) {
		fmt.Printf("sub3: received message %q\n", msg.Subject)
		msg.Ack()
		wg.Done()
	})

	wg.Wait()

}

func printStreamState(js nats.JetStreamContext, name string) {
	info, _ := js.StreamInfo(name)
	b, _ := json.MarshalIndent(info.State, "", " ")
	fmt.Println("inspecting stream info")
	fmt.Println(string(b))
}
