package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"syscall"

	// "strings"

	"github.com/Shopify/sarama"
)

var refList = map[string]map[string]Comment{}
var serverList = map[string]map[string]map[string]string{}
var sent = false

const s = 4

const n = 6
const serverId = "shared"

type Comment struct {
	Text   string `form:"text" json:"text"`
	Id     string `form:"id" json:"id"`
	NumInc string `form:"num-inc" json:"num-inc"`
}
type MessageSent struct {
	Text     string            `form:"text" json:"text"`
	NumInc   string            `form:"num-inc" json:"num-inc"`
	IdList   map[string]string `form:"idList" json:"idList"`
	ServerId string            `form:"serverId" json:"serverId"`
}

func main() {
	topic := "shared"
	topic2 := "servers"
	topic3 := "clients"

	worker, err := connectConsumer([]string{"127.0.0.1:9092"})
	if err != nil {
		panic(err)
	}

	// Calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	consumer2, err2 := worker.ConsumePartition(topic2, 0, sarama.OffsetOldest)
	consumer3, err2 := worker.ConsumePartition(topic3, 0, sarama.OffsetOldest)

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

				msgCount++

				fmt.Printf("Received message Count %d: | Topic(%s) | Message(%s) \n", msgCount, string(msg.Topic), string(msg.Value))

			case <-sigchan:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			case err := <-consumer2.Errors():
				fmt.Println(err)
			case msg := <-consumer2.Messages():
				bytes := []byte(string(msg.Value))
				var message MessageSent
				json.Unmarshal(bytes, &message)
				setServerList(message)

			case msg3 := <-consumer3.Messages():
				if msg3.Value != nil {
					content := Comment{}
					json.Unmarshal(msg3.Value, &content)
					if content.Text != "" && content.Id != "" && content.NumInc != "" {
						setRefList(content)

					}

				}

				msgCount++

				// fmt.Printf("Received message Count %d: | Topic(%s) | Message(%s) \n", msgCount, string(msg3.Topic), string(msg3.Value))

			}
		}

	}()

	<-doneCh
	// fmt.Println("Processed", msgCount, "messages")

	if err := worker.Close(); err != nil {
		panic(err)
	}

}

func setServerList(msg MessageSent) {

	serverList[msg.ServerId] = make(map[string]map[string]string)
	serverList[msg.ServerId][msg.NumInc] = msg.IdList

	if getServersNumber(msg) >= s/2 {

		fmt.Println("work")

	}

}
func getServersNumber(msg MessageSent) int {
	var i = 0
	for key, mapRef := range serverList {
		for id, _ := range mapRef {
			if id == msg.NumInc && reflect.DeepEqual(msg.IdList, serverList[key][id]) == true {
				i++

			}

		}

	}
	fmt.Println(i)
	return i

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
	if sent == false {

		_, ok := refList[msg.NumInc]
		if len(refList) == 0 || true {
			if !ok {
				refList[msg.NumInc] = make(map[string]Comment)
			}
			refList[msg.NumInc][msg.Id] = msg

		}
		if len(refList[msg.NumInc]) >= n/2 {
			var list = map[string]string{}
			var i = 0

			for key, _ := range refList[msg.NumInc] {
				// fmt.Println("Key:", key, "=>", "Element:", element)
				list[key] = key
				i = i + 1
				// fmt.Println(list)
			}

			var msg_sent = MessageSent{}
			msg_sent.IdList = list
			msg_sent.NumInc = msg.NumInc
			msg_sent.Text = msg.Text
			msg_sent.ServerId = serverId

			cmtInBytes, _ := json.Marshal(msg_sent)
			SendMessageToServers("servers", cmtInBytes)
			sent = true

		}

	}
}
func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
func SendMessageToServers(topic string, message []byte) error {

	brokersUrl := []string{"127.0.0.1:9092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
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
