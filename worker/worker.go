package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
)

var refList = map[string]map[string]Comment{}

const n = 20

type Comment struct {
	Text   string `form:"text" json:"text"`
	Id     string `form:"id" json:"id"`
	NumInc string `form:"num-inc" json:"num-inc"`
}

func main() {
	topic := "comments"
	topic2 := "servers"

	worker, err := connectConsumer([]string{"127.0.0.1:9092"})
	if err != nil {
		panic(err)
	}

	// Calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	consumer2, err2 := worker.ConsumePartition(topic2, 0, sarama.OffsetOldest)

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

				fmt.Printf("Topic(%s) | Message(%s) \n", string(msg.Topic), string(msg.Value))
				fmt.Printf("recieved message from servers")
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
	fmt.Println(len(refList[msg.NumInc]))
	if len(refList[msg.NumInc]) > n/2 {
		fmt.Println("un message")
		content := msg
		cmtInBytes, _ := json.Marshal(content)
		SendMessageToServers("servers", cmtInBytes)
		fmt.Println("message sent")

	}
	fmt.Println(refList)

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
