package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"syscall"
)

var refList = map[string]map[string]Comment{}

type Comment struct {
	Text   string `form:"text" json:"text"`
	Id     string `form:"id" json:"id"`
	NumInc string `form:"num-inc" json:"num-inc"`
}

func main() {

	//myMap2[cmt.Id] = append(myMap2[cmt.Id], Comment{Text: "azza"})
	//myMap[cmt.NumInc] = append(myMap[cmt.NumInc], myMap2)
	////myMap[2] = append(myMap[2], Comment{"azza", "2"})
	//fmt.Println(myMap)
	topic := "comments"
	topic2 := "servers"

	worker, err := connectConsumer([]string{"127.0.0.1:9092"})
	if err != nil {
		panic(err)
	}

	// Calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.
	consumer, err := worker.ConsumePartition(topic2, 0, sarama.OffsetOldest)
	consumer2, err2 := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)

	if err != nil || err2 != nil {
		panic(err)

	}
	fmt.Println("Consumer started ")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// Count how many message processed
	msgCount := 0

	// Get signal for finish
	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():

				if msg.Value != nil {
					content := Comment{}
					json.Unmarshal(msg.Value, &content)
					if content.Text != "" && content.Id != "" && content.NumInc != "" {
						setRefList(content)

					}

				}
				//m := string(msg.Value)

				//fmt.Printf("%+v\n", content.Text)
				msgCount++
				fmt.Printf("Received message Count %d: | Topic(%s) | Message(%s) \n", msgCount, string(msg.Topic), string(msg.Value))

			case <-sigchan:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			case err := <-consumer2.Errors():
				fmt.Println(err)
			case msg := <-consumer2.Messages():

				fmt.Printf("Topic(%s) | Message(%s) \n", string(msg.Topic), string(msg.Value))
			}
		}

	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")

	if err := worker.Close(); err != nil {
		panic(err)
	}

}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create new consumer
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
func setRefList(msg Comment) {

	_, ok := refList[msg.NumInc]
	if len(refList) == 0 || true {
		if !ok {
			refList[msg.NumInc] = make(map[string]Comment)
		}
		refList[msg.NumInc][msg.Id] = msg

	}
	fmt.Println(refList)

}

//func exist(msg Comment) bool {
//	val, ok := refList[msg.NumInc]
//	if ok {
//		_, okMsg := refList[msg.NumInc]
//		return okMsg
//
//	}
//	return false
//}
