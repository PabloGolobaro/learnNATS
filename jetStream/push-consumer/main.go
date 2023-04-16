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

	streamName := "EVENTS"

	cfg := &nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{"events.>"},
	}

	js.AddStream(cfg)

	js.Publish("events.1", nil)
	js.Publish("events.2", nil)
	js.Publish("events.3", nil)

	fmt.Println("# Ephemeral")
	sub, _ := js.SubscribeSync("events.>", nats.AckExplicit())

	ephemeralName := <-js.ConsumerNames(streamName)
	fmt.Printf("ephemeral name is %q\n", ephemeralName)

	queuedMsgs, _, _ := sub.Pending()
	fmt.Printf("%d messages queued\n", queuedMsgs)

	js.Publish("events.4", nil)
	js.Publish("events.5", nil)
	js.Publish("events.6", nil)

	queuedMsgs, _, _ = sub.Pending()
	fmt.Printf("%d messages queued\n", queuedMsgs)

	msg, _ := sub.NextMsg(time.Second)
	fmt.Printf("received %q\n", msg.Subject)

	msg.Ack()

	msg, _ = sub.NextMsg(time.Second)
	fmt.Printf("received %q\n", msg.Subject)
	msg.Ack()

	queuedMsgs, _, _ = sub.Pending()
	fmt.Printf("%d messages queued\n", queuedMsgs)

	sub.Unsubscribe()

	fmt.Println("\n# Durable (Helper)")

	sub, _ = js.SubscribeSync("events.>", nats.Durable("handler-1"), nats.AckExplicit())

	queuedMsgs, _, _ = sub.Pending()
	fmt.Printf("%d messages queued\n", queuedMsgs)

	msg, _ = sub.NextMsg(time.Second)
	msg.Ack()

	sub.Unsubscribe()

	_, err := js.ConsumerInfo("EVENTS", "handler-1")
	fmt.Println(err)

	fmt.Println("\n# Durable (AddConsumer)")

	consumerName := "handler-2"
	js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:        consumerName,
		DeliverSubject: "handler-2",
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        time.Second,
	})

	sub, _ = js.SubscribeSync("", nats.Bind(streamName, consumerName))

	msg, _ = sub.NextMsg(time.Second)
	fmt.Printf("received %q\n", msg.Subject)

	msg.Ack()
	queuedMsgs, _, _ = sub.Pending()
	fmt.Printf("%d messages queued\n", queuedMsgs)

	sub.Unsubscribe()

	info, _ := sub.ConsumerInfo()
	fmt.Printf("max stream sequence delivered: %d\n", info.Delivered.Stream)
	fmt.Printf("max consumer sequence delivered: %d\n", info.Delivered.Consumer)
	fmt.Printf("num ack pending: %d\n", info.NumAckPending)
	fmt.Printf("num redelivered: %d\n", info.NumRedelivered)

	sub, _ = js.SubscribeSync("", nats.Bind(streamName, consumerName))
	_, err = sub.NextMsg(100 * time.Millisecond)
	fmt.Printf("received timeout? %v\n", err == nats.ErrTimeout)

	msg, _ = sub.NextMsg(time.Second)
	md, _ := msg.Metadata()
	fmt.Printf("received %q (delivery #%d)\n", msg.Subject, md.NumDelivered)
	msg.Ack()

	info, _ = sub.ConsumerInfo()
	fmt.Printf("max stream sequence delivered: %d\n", info.Delivered.Stream)
	fmt.Printf("max consumer sequence delivered: %d\n", info.Delivered.Consumer)
	fmt.Printf("num ack pending: %d\n", info.NumAckPending)
	fmt.Printf("num redelivered: %d\n", info.NumRedelivered)

}

func printStreamState(js nats.JetStreamContext, name string) {
	info, _ := js.StreamInfo(name)
	b, _ := json.MarshalIndent(info.State, "", " ")
	fmt.Println("inspecting stream info")
	fmt.Println(string(b))
}
