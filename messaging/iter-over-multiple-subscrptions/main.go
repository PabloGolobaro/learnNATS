package main

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

func main() {
	var wg sync.WaitGroup

	logger := log.New(os.Stdout, "", log.Lshortfile)

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, _ := nats.Connect(url)

	defer nc.Drain()

	sub_car, err := nc.SubscribeSync("cars.>")
	if err != nil {
		logger.Println(err)
	}
	sub_plane, err := nc.SubscribeSync("planes.>")
	if err != nil {
		logger.Println(err)
	}

	sub_ship, err := nc.SubscribeSync("ships.>")
	if err != nil {
		logger.Println(err)
	}

	s := map[int]*nats.Subscription{0: sub_car, 1: sub_plane, 3: sub_ship}

	for i := 0; i < 3; i++ {
		go func(num int) {
			var msg *nats.Msg
			var err error
			for {
				msg, err = s[num].NextMsg(time.Millisecond * 10)
				if err != nil {
					logger.Println(err)
					break
				}
				fmt.Printf("Got message:(%s) in goroutine %d\n", string(msg.Data), num)
			}
		}(i)
	}

	m := map[int]string{0: "cars", 1: "planes", 2: "ships"}

	wg.Add(3)

	for i := 0; i < 3; i++ {
		go func(num int) {
			subj := m[num] + "."

			for i := 0; i < 100; i++ {

				resSubj := subj + strconv.Itoa(i)
				data := subj + " number " + strconv.Itoa(i)

				err := nc.Publish(resSubj, []byte(data))
				if err != nil {
					logger.Println(err)
				}
				fmt.Printf("Published message:(%s) in goroutine %d\n", data, num)
			}

			wg.Done()
		}(i)
	}

	wg.Wait()

}
