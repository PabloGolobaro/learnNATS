package main

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
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

	_, err := nc.Subscribe("greet", func(msg *nats.Msg) {
		var req GreetRequest
		err := proto.Unmarshal(msg.Data, &req)
		if err != nil {
			logger.Println(err)
		}

		rep := GreetReply{
			Text: fmt.Sprintf("hello %q!", req.Name),
		}
		data, _ := proto.Marshal(&rep)
		err = msg.Respond(data)
		if err != nil {
			logger.Println(err)
		}
	})
	if err != nil {
		logger.Println(err)
	}

	req := GreetRequest{
		Name: "joe",
	}
	data, _ := proto.Marshal(&req)

	msg, _ := nc.Request("greet", data, time.Second)

	var rep GreetReply
	err = proto.Unmarshal(msg.Data, &rep)
	if err != nil {
		logger.Println(err)
	}

	fmt.Printf("reply: %s\n", rep.Text)
}
