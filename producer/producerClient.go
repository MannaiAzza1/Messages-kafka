package main

import (
	"bufio"
	"encoding/json"
	"fmt"

	"os"
	"strings"

	"github.com/Shopify/sarama"
)

// Comment struct
type Comment struct {
	Text   string `form:"text" json:"text"`
	Id     string `form:"id" json:"id"`
	NumInc string `form:"num-inc" json:"num-inc"`
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Producer Started")
	fmt.Println("---------------------")

	for {
		fmt.Println("Please write the message content")
		content, _ := reader.ReadString('\n')

		content = strings.Replace(content, "\r\n", "", -1)

		// convert CRLF to LF

		fmt.Println("Please write the message id")
		id, _ := reader.ReadString('\n')
		id = strings.Replace(id, "\r\n", "", -1)
		fmt.Println("Please write the message Instance")
		ins, _ := reader.ReadString('\n')
		ins = strings.Replace(id, "\r\n", "", -1)

		cmt := new(Comment)
		cmt.Id = id
		cmt.NumInc = ins
		cmt.Text = content
		fmt.Printf("message %s id %s num inc %s", cmt.Id, cmt.NumInc, cmt.Text)
		createComment(*cmt)

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

func PushCommentToQueue(topic string, message []byte) error {

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

// createComment handler
func createComment(cmt Comment) {

	cmtInBytes, err := json.Marshal(cmt)
	PushCommentToQueue("shared", cmtInBytes)
	if err != nil {
		fmt.Println("error pushing")
	}

	// Return Comment in JSON format

}
