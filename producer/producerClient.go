package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"strings"
)

const topic = "clients" //definir le nom du topic kafka

type StoreReqMsg struct { //envoyé par un client à tous les serveurs
	Data         string `form:"text" json:"text"`                   //contenu utile chaîne de caractères arbitraire
	CID          string `form:"id" json:"id"`                       //  numéro d’instance chiffre entre 1 et L (c’est l’ID du client expéditeur)
	SrNb         string `form:"num-inc" json:"num-inc"`             //chiffre arbitraire, distinct pour chaque StoreReq
	HighPriority bool   `form:"high_priority" json:"high_priority"` // flag  false par défaut, true sinon
	SigC         string `form:"sigc" json:"sigc"`                   // signature du client
}

const UrlKafka = "127.0.0.1:9092" //  l'url du serveur kafka
// fonction main
func main() {
	ReadAndCreateCommentFromConsole()
}

// fonction pour lire les variables et ecrire sur le console
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
		id, _ := reader.ReadString('\n') //lire l'id du console
		id = strings.Replace(id, "\r\n", "", -1)
		fmt.Println("Please write the message Instance")
		ins, _ := reader.ReadString('\n') //lire le data du console
		ins = strings.Replace(ins, "\r\n", "", -1)
		fmt.Println("Please write the message priority (true/false)")
		priority, _ := reader.ReadString('\n') //lire la priorité du console
		priority = strings.Replace(priority, "\r\n", "", -1)
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
		createAndSendStoreReqMessage(*MessageToSend) // creation et envoi du message

	}

}

// fonction pour intialiser les paramteres du serveur en tant que producteur des messages(envoyer des messages)

func initializeSenderForMessages(brokersUrl []string) (sarama.SyncProducer, error) {
	// Paramétrage du serveur Kafka
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

// envoie msg à tous les serveurs
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

// fonction pour seraliser le message et l'envoyer à tous les serveurs
func createAndSendStoreReqMessage(msg StoreReqMsg) {

	cmtInBytes, err := json.Marshal(msg) // seraliser le message en json( nécessaire pour l'envoi)
	SendToAllServers(topic, cmtInBytes)  // Appel à la fonction d'envoi
	if err != nil {
		fmt.Println("error pushing")
	} // en cas d'erreur

}
