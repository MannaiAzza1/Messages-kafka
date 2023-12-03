package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/gofiber/fiber/v2"
)

// StoreReqMsg struct
type ClientToServerMsg struct {
	Data string `form:"text" json:"text"`
	CID  string `form:"id" json:"id"`
	SrNb string `form:"num-inc" json:"num-inc"`
}

func main() {
	fmt.Println("Client Started")
	app := fiber.New()
	api := app.Group("/api/v1") // /api
	api.Post("/server1", createAndSendMessage)
	app.Listen(":3001")

}

func initializeSenderForMessagesToServer(brokersUrl []string) (sarama.SyncProducer, error) {

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

func SendToServer(topic string, message []byte) error {

	brokersUrl := []string{UrlKafka}
	producer, err := initializeSenderForMessagesToServer(brokersUrl)
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
func createAndSendMessage(c *fiber.Ctx) error {

	// Instantiate new Message struct
	messageToServer := new(ClientToServerMsg)

	//  Parse body into message struct
	if err := c.BodyParser(messageToServer); err != nil {
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}
	// convert body into bytes and send it to kafka
	messageInBytes, err := json.Marshal(messageToServer)
	SendToServer("server1", messageInBytes)

	// Return Message in JSON format
	err = c.JSON(&fiber.Map{
		"success": true,
		"message": "StoreReqMsg pushed successfully",
		"comment": messageToServer,
	})
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error creating product",
		})
		return err
	}

	return err
}
