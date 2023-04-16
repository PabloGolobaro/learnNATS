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

	consumerName := "processor"
	ackWait := 10 * time.Second
	ackPolicy := nats.AckExplicitPolicy
	maxWaiting := 1

	js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:    consumerName,
		AckPolicy:  ackPolicy,
		AckWait:    ackWait,
		MaxWaiting: maxWaiting,
	})

	sub, _ := js.PullSubscribe("", consumerName, nats.Bind(streamName, consumerName))

	fmt.Println("--- max in-flight messages (n=1) ---")

	js.UpdateConsumer(streamName, &nats.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     ackPolicy,
		AckWait:       ackWait,
		MaxWaiting:    maxWaiting,
		MaxAckPending: 1,
	})

	js.Publish("events.1", nil)
	js.Publish("events.2", nil)

	msgs, _ := sub.Fetch(3)
	fmt.Printf("requested 3, got %d\n", len(msgs))

	_, err := sub.Fetch(1, nats.MaxWait(time.Second))
	fmt.Printf("%s\n", err)

	msgs[0].Ack()

	msgs, _ = sub.Fetch(1)
	fmt.Printf("requested 1, got %d\n", len(msgs))
	msgs[0].Ack()

	fmt.Println("\n--- max fetch batch size (n=2) ---")

	js.UpdateConsumer(streamName, &nats.ConsumerConfig{
		Durable:         consumerName,
		AckPolicy:       ackPolicy,
		AckWait:         ackWait,
		MaxWaiting:      maxWaiting,
		MaxRequestBatch: 2,
	})

	js.Publish("events.1", []byte("hello"))
	js.Publish("events.2", []byte("world"))

	msgs, _ = sub.Fetch(2)
	fmt.Printf("requested 2, got %d\n", len(msgs))

	msgs[0].Ack()
	msgs[1].Ack()

	fmt.Println("\n--- max waiting requests (n=1) ---")

	js.UpdateConsumer(streamName, &nats.ConsumerConfig{
		Durable:    consumerName,
		AckPolicy:  ackPolicy,
		AckWait:    ackWait,
		MaxWaiting: maxWaiting,
	})

	fmt.Printf("is valid? %v\n", sub.IsValid())

	wg := &sync.WaitGroup{}
	wg.Add(3)

	go func() {
		_, err := sub.Fetch(1, nats.MaxWait(time.Second))
		fmt.Printf("fetch 1: %s\n", err)
		wg.Done()
	}()

	go func() {
		_, err := sub.Fetch(1, nats.MaxWait(time.Second))
		fmt.Printf("fetch 2: %s\n", err)
		wg.Done()
	}()

	go func() {
		_, err := sub.Fetch(1, nats.MaxWait(time.Second))
		fmt.Printf("fetch 3: %s\n", err)
		wg.Done()
	}()

	wg.Wait()

	fmt.Println("\n--- max fetch timeout (d=1s) ---")

	js.UpdateConsumer(streamName, &nats.ConsumerConfig{
		Durable:           consumerName,
		AckPolicy:         ackPolicy,
		AckWait:           ackWait,
		MaxWaiting:        maxWaiting,
		MaxRequestExpires: time.Second,
	})

	t0 := time.Now()
	_, err = sub.Fetch(1, nats.MaxWait(time.Second))
	fmt.Printf("timeout occured? %v in %s\n", err == nats.ErrTimeout, time.Since(t0))

	t0 = time.Now()
	_, err = sub.Fetch(1, nats.MaxWait(5*time.Second))
	fmt.Printf("%s in %s\n", err, time.Since(t0))

	fmt.Println("\n--- max total bytes per fetch (n=4) ---")

	js.UpdateConsumer(streamName, &nats.ConsumerConfig{
		Durable:            consumerName,
		AckPolicy:          ackPolicy,
		AckWait:            ackWait,
		MaxWaiting:         maxWaiting,
		MaxRequestMaxBytes: 3,
	})

	js.Publish("events.3", []byte("hola"))
	js.Publish("events.4", []byte("again"))

	_, err = sub.Fetch(2, nats.PullMaxBytes(4))
	fmt.Printf("%s\n", err)
}

func printStreamState(js nats.JetStreamContext, name string) {
	info, _ := js.StreamInfo(name)
	b, _ := json.MarshalIndent(info.State, "", " ")
	fmt.Println("inspecting stream info")
	fmt.Println(string(b))
}
