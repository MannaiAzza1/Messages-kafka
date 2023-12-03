package main

import (
	"encoding/json" //librarie utilisée pour le marshal/unmarshal des objets
	"fmt"           // utiliser pour l'affichages des message
	"os"
	"reflect" // utilisé pour compater si deux maps sont identiques avec la methode deepEquals
	// "strings"

	"github.com/Shopify/sarama"
)

var refList = map[string]map[string]StoreReqMsg{} // la liste des message recu par le serveur et envoyée par des clients , regroupées par numero d'instance , sans redondance
var serversList = map[string]map[string]map[string]string{}
var sentFlag = false // variable pour s'assurer que le serveur diffuse son message à tous les autres serveurs qu'une seule fois
const UrlKafka = "127.0.0.1:9092"
const s = 4 // represente le variable s qui est egale à N/2 :

const n = 6
const serverId = "server1"

// cet fonction est pour intialiser les paramteres du serveur en tant que producteur des messages
func initializeSenderForMessages(brokersUrl []string) (sarama.SyncProducer, error) {

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

type StoreReqMsg struct {
	Data   string `form:"text" json:"text"`
	CID    string `form:"id" json:"id"`
	NumInc string `form:"num-inc" json:"num-inc"`
}
type AckStoreMsg struct {
	Text     string            `form:"text" json:"text"`
	SrNb     string            `form:"num-inc" json:"num-inc"`
	CinitID  string            `form:"cinitID" json:"cinitID"`
	IdList   map[string]string `form:"idList" json:"idList"`
	SID      string            `form:"serverId" json:"serverId"`
	SigCinit string            `form:"sigCinit" json:"sigCinit"`
	SigServ  string            `form:"sigServ" json:"sigServ"`
}

func main() {
	initializekafkaServer()

}
func initializeListenerForMessages(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create new consumer
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func initializekafkaServer() {
	toServer := serverId            //sujet specifique à ce serveur sur lequel tous les clients peuvent envoyée des messages
	serverToAllServers := "servers" //sujet pour tous les serveurs
	clientToAllServers := "clients" // sujets pour les message envoyées à partir d'un client à tous les serveurs

	worker, err := initializeListenerForMessages([]string{UrlKafka}) //spécifier url du serveur kafka
	if err != nil {
		panic(err)
	}

	// creation des consommateurs pour chaque sujet
	toServerConsumer, err := worker.ConsumePartition(toServer, 0, sarama.OffsetOldest)
	serverToAllServersConsumer, err := worker.ConsumePartition(serverToAllServers, 0, sarama.OffsetOldest)
	clientToAllServersConsumer, err := worker.ConsumePartition(clientToAllServers, 0, sarama.OffsetOldest)

	if err != nil {
		panic(err)

	} // test sur les erreurs de démrrage du consommateurs
	fmt.Println("Consumer started ")
	handleRecieveMessages(toServerConsumer, serverToAllServersConsumer, clientToAllServersConsumer, worker)
}

func handleRecieveMessages(toServerConsumer sarama.PartitionConsumer, serverToAllServersConsumer sarama.PartitionConsumer, clientToAllServersConsumer sarama.PartitionConsumer, worker sarama.Consumer) {
	sigchan := make(chan os.Signal, 1)

	// singaler fin
	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-toServerConsumer.Errors():
				fmt.Println(err)
			case receivedMessageToServer := <-toServerConsumer.Messages(): // cas ou le consommateur recoit un message

				if receivedMessageToServer.Value != nil { // si son valeur est different de null
					content := StoreReqMsg{}
					json.Unmarshal(receivedMessageToServer.Value, &content)              //transformer en format json
					if content.Data != "" && content.CID != "" && content.NumInc != "" { //si tous les champs sont pas vides on fait appel à la fonction refList
						HandleStoreReqMsg(content)
						fmt.Printf("Received message specific to this server:  Topic(%s) | Message(%s) \n", string(receivedMessageToServer.Topic), string(receivedMessageToServer.Value))

					}

				}

			case receivedMessage := <-serverToAllServersConsumer.Messages(): // si le serveur recoit des message à partir du client
				bytes := []byte(string(receivedMessage.Value))
				var message AckStoreMsg
				fmt.Printf("Received message specific to all servers:  Topic(%s) | Message(%s) \n", string(receivedMessage.Topic), string(receivedMessage.Value))
				json.Unmarshal(bytes, &message)
				HandleAckStoreMsg(message) //fait appel a la methode setServerList

			case receivedMessageFromClient := <-clientToAllServersConsumer.Messages():
				if receivedMessageFromClient.Value != nil {
					content := StoreReqMsg{}
					fmt.Printf("Received message specific to all servers from client:  Topic(%s) | Message(%s) \n", string(receivedMessageFromClient.Topic), string(receivedMessageFromClient.Value))
					json.Unmarshal(receivedMessageFromClient.Value, &content)
					if content.Data != "" && content.CID != "" && content.NumInc != "" {
						HandleStoreReqMsg(content)

					}

				}
			case <-sigchan:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			case err := <-serverToAllServersConsumer.Errors():
				fmt.Println(err)

			}
		}

	}()

	<-doneCh
	if err := worker.Close(); err != nil {
		panic(err)
	}

}

func HandleAckStoreMsg(msg AckStoreMsg) {

	serversList[msg.SID] = make(map[string]map[string]string)

	if getServersNumber(msg) >= s/2 {

		fmt.Println("work")

	}

}
func getServersNumber(msg AckStoreMsg) int {
	var i = 0
	for key, mapRef := range serversList {
		for id, _ := range mapRef {
			if id == msg.SrNb && reflect.DeepEqual(msg.IdList, serversList[key][id]) == true {
				i++

			}

		}

	}
	return i

}

func HandleStoreReqMsg(msg StoreReqMsg) { //la fonction prend en parametre le message recu par le serveur à partir d'un client
	if sentFlag == false { // tester si il a déja envoyée un message avant

		_, ok := refList[msg.NumInc]
		if len(refList) == 0 || true { // si la liste est vide donc on va ajouter directement le message dans la liste
			if !ok { // si dans le map il n'existe pas une case avec cet numero d'instance
				refList[msg.NumInc] = make(map[string]StoreReqMsg) //intialisation du case
			}
			refList[msg.NumInc][msg.CID] = msg //on va ajouter aux map groupées par le numero d'instance le message recu
			//exemple : reflist : [100 :[1:{id:1,numInc:100, text :"message1"}] , 200[5:{id:5,numInc:200,text:"message 5"}]]

		}
		if len(refList[msg.NumInc]) >= n/2 { // si les nombres de messages recu d'un meme numero d'instance est > à n/2 le serveur va diffuser un message à tous les autres serveurs
			var list = map[string]string{} // prepation de la liste des messages recu par le serveur exemple : [5:5,1:1]
			var i = 0

			for key, _ := range refList[msg.NumInc] { //parcourir la liste des message par numero d'instance
				// fmt.Println("Key:", key, "=>", "Element:", element)
				list[key] = key
				i = i + 1
				// fmt.Println(list)
			}
			//intialisation du contenu du message à envoyer

			var messageToSend = AckStoreMsg{}
			messageToSend.IdList = list
			messageToSend.SrNb = msg.NumInc
			messageToSend.Text = msg.Data
			messageToSend.SID = serverId

			cmtInBytes, _ := json.Marshal(messageToSend) // compresser le message avant de l'envoyer
			SendToAllServers("servers", cmtInBytes)      // publier le message dans kafka
			sentFlag = true                              //changer la valeur du variable à true

		}

	}
}

// fonction d'envoi d'un message à tous les serveurs : elle prend comme paramétre le nom du sujet kafka et le message à envoyer
func SendToAllServers(topic string, message []byte) error {

	brokersUrl := []string{UrlKafka}
	producer, err := initializeSenderForMessages(brokersUrl)
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
