package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"github.com/Shopify/sarama"
)

const topic = "clients" //definir le nom du topic kafka

type StoreReqMsg struct {
	Data         string `form:"text" json:"text"`
	CID          string `form:"id" json:"id"`
	SrNb         string `form:"num-inc" json:"num-inc"`
	HighPriority bool   `form:"high_priority" json:"high_priority"`
	SigC         string `form:"sigc" json:"sigc"`
}

const UrlKafka = "127.0.0.1:9092" // definir l'url du serveur kafka 

func main() {
	ReadAndCreateCommentFromConsole()
}

func ReadAndCreateCommentFromConsole() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Producer Started")
	fmt.Println("---------------------")

	for { // boucle pour l'affichage et la lecture des valeurs saisie à travers le console
		fmt.Println("Please write the message content")
		content, _ := reader.ReadString('\n')

		content = strings.Replace(content, "\r\n", "", -1)

		// convert CRLF to LF

		fmt.Println("Please write the message id")
		id, _ := reader.ReadString('\n')
		id = strings.Replace(id, "\r\n", "", -1)
		fmt.Println("Please write the message Instance")
		ins, _ := reader.ReadString('\n')
		ins = strings.Replace(ins, "\r\n", "", -1)
		fmt.Println("Please write the message priority (true/false)")
		priority, _ := reader.ReadString('\n')
		priority = strings.Replace(priority, "\r\n", "", -1)
		fmt.Println(priority)
		MessageToSend := new(StoreReqMsg) // prepartion du message a travers les champs remplit en console
		MessageToSend.CID = id
		MessageToSend.SrNb = ins
		MessageToSend.Data = content
		if priority == "true" {
			MessageToSend.HighPriority = true
		} else {
			MessageToSend.HighPriority = false
		}
		fmt.Printf("message-c %s id-m %s num inc %s", MessageToSend.CID, MessageToSend.SrNb, MessageToSend.Data)
		createAndSendStoreReqMessage(*MessageToSend) // creation du message

	}

}

func initializeSenderForMessages(brokersUrl []string) (sarama.SyncProducer, error) { // Paramétrage du serveur Kafka

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

func SendToAllServers(topic string, message []byte) error {

	brokersUrl := []string{UrlKafka}                         // intialisation de l'url du serveur kafka
	producer, err := initializeSenderForMessages(brokersUrl) // intialisation du  parametre kafka pour l'envoi du message
	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	} // parameter le message à envoyer : on specifie la valeur du message ainsi que le topic (l'ID du serveur) en utilisant la bibliotheque Sarama

	partition, offset, err := producer.SendMessage(msg) // envoyer le message
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}

// createComment handler
func createAndSendStoreReqMessage(cmt StoreReqMsg) {

	cmtInBytes, err := json.Marshal(cmt) // seraliser le message en json( nécessaire pour l'envoi)
	SendToAllServers(topic, cmtInBytes)  // Appel à la fonction d'envoi
	if err != nil {
		fmt.Println("error pushing")
	} // en cas d'erreur

}
