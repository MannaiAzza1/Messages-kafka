package main

import (
	"encoding/json" //librarie utilisée pour le marshal/unmarshal des objets
	"fmt"           // utiliser pour l'affichages des message
	"os"
	"reflect" // utilisé pour comparer si deux maps sont identiques avec la methode deepEquals
	// "strings"

	"github.com/Shopify/sarama"
)

var messagesReferenceList = map[string]map[string]AckStoreMsg{} // la liste des message recu par le serveur et envoyée par des clients , regroupées par numero d'instance , sans redondance
var serversList = map[string]map[string]map[string]string{} /// la liste des serveurs 
var sentFlag = false // variable pour s'assurer que le serveur diffuse son message à tous les autres serveurs qu'une seule fois
const UrlKafka = "127.0.0.1:9092" // l'url du serveur kafka
const s = 2 // represente le variable s qui est egale à N/2 :
var storeResMessagesList = map[string]map[int]StoreReqMsg{}
var topicsClients = map[string]string{}//liste des topics pour les clients
var message = make(map[int]AckStoreMsg)

const t = 2
const n = t*2 + 1
const serverId = "server1"

var AckStoreMessagesList = map[string]map[int]AckStoreMsg{}

type Cert struct {
	QuorumSigs map[string]string
	CinitID1   string
	SrNb       string
}
type StoreReqMsg struct {
	Data         string `form:"text" json:"text"`
	CID          string `form:"id" json:"id"`
	SrNb         string `form:"num-inc" json:"num-inc"`
	HighPriority bool   `form:"high_priority" json:"high_priority"`
	SigC         string `form:"sigc" json:"sigc"`
}
type AckStoreMsg struct {
	Data     string            `form:"text" json:"text"`
	SrNb     string            `form:"num-inc" json:"num-inc"`
	CinitID  string            `form:"cinitID" json:"cinitID"`
	IdList   map[string]string `form:"idList" json:"idList"`
	SID      string            `form:"serverId" json:"serverId"`
	SigCinit string            `form:"sigCinit" json:"sigCinit"`
	SigServ  string            `form:"sigServ" json:"sigServ"`
}

// cette fonction est pour intialiser les paramteres du serveur en tant que producteur des messages
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
func main() {
	topicsClients["client1"] = "client1"
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

	} // test sur les erreurs de démarrage des consommateurs
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
					json.Unmarshal(receivedMessageToServer.Value, &content)            //transformer en format json
					if content.Data != "" && content.CID != "" && content.SrNb != "" { //si tous les champs sont pas vides on fait appel à la fonction messagesReferenceList
						HandleStoreReqMsg(content)
						fmt.Printf("Received message specific to this server:  Topic(%s) | Message(%s) \n", string(receivedMessageToServer.Topic), string(receivedMessageToServer.Value))

					}

				}

			case receivedMessage := <-serverToAllServersConsumer.Messages(): // si le serveur recoit des message à partir d'un serveur
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
					if content.Data != "" && content.CID != "" && content.SrNb != "" {
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
func HandleAckStoreMsgOld(msg AckStoreMsg) {

	serversList[msg.SID] = make(map[string]map[string]string)
	message[len(AckStoreMessagesList[msg.SID])+1] = msg
	if len(AckStoreMessagesList[msg.SID]) > t+1 {
		CreateStoreCertif(AckStoreMessagesList)

	}

	if getServersNumber(msg) >= s/2 {

		fmt.Println("work")

	}

}

func CreateStoreCertif(list map[string]map[int]AckStoreMsg) Cert {
	var i = 1
	var output Cert
	var messages = map[int]AckStoreMsg{}
	var quorumSigs = map[string]string{}
	var amsg = AckStoreMsg{}

	for _, server := range list {
		for _, message := range server {
			messages[i] = message
			i = i + 1
		}
		amsg = messages[1]
		for _, message := range messages {
			if message.Data == amsg.Data {
				quorumSigs[message.SID] = message.SigServ

			} else {
				fmt.Println("client cinitID1 corrompu")

			}
		}

	}
	output.QuorumSigs = quorumSigs
	output.CinitID1 = amsg.CinitID
	output.SrNb = amsg.SrNb
	return output
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
func SendToClient(ackMsg []byte, cID string) error {
	brokersUrl := []string{UrlKafka}
	var topic = topicsClients[cID]
	producer, err := initializeSenderForMessages(brokersUrl)
	if err != nil {
		return err
	}

	defer producer.Close()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(ackMsg),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}
func HandleStoreReqMsg(msg StoreReqMsg) {
	_, exist := storeResMessagesList[msg.SrNb]
	if !exist || (checkForMsgId(msg, storeResMessagesList[msg.SrNb]) == false) {
		if msg.HighPriority == true {
			fmt.Println("my flag is true")
			var msgConvert = AckStoreMsg{SID: serverId, SrNb: msg.SrNb, CinitID: msg.CID, Data: msg.Data}
			cmtInBytes, _ := json.Marshal(msgConvert)
			SendToAllServers(cmtInBytes)
		}
		fmt.Println("traitement to do in all cases")
		var asmsg = AckStoreMsg{SID: serverId, SrNb: msg.SrNb, CinitID: msg.CID, Data: msg.Data}
		asmsgInBytes, _ := json.Marshal(asmsg)
		SendToAllServers(asmsgInBytes)
		SendToClient(asmsgInBytes, msg.CID)

	}
	var message = make(map[int]StoreReqMsg)
	message[len(storeResMessagesList[msg.SrNb])+1] = msg
	storeResMessagesList[msg.SrNb] = message

}

func checkForMsgId(msg StoreReqMsg, m map[int]StoreReqMsg) bool {
	var isPresent = false
	for _, message := range m {
		if msg.CID == message.CID {
			isPresent = true
		}

	}
	return isPresent
}
func HandleAckStoreMsg(msg AckStoreMsg) {
	serversList[msg.SID] = make(map[string]map[string]string)
	message[len(AckStoreMessagesList[msg.SID])+1] = msg
	AckStoreMessagesList[msg.SID] = message

	if len(AckStoreMessagesList[msg.SID]) > t+1 {
		CreateStoreCertif(AckStoreMessagesList)

	}

	if msg.SID != "" {
		_, ok := messagesReferenceList[msg.SrNb]
		if len(messagesReferenceList) == 0 || true {
			if !ok {
				messagesReferenceList[msg.SrNb] = make(map[string]AckStoreMsg)
			}
			messagesReferenceList[msg.SrNb][msg.CinitID] = msg
		}
		var distinctServersNum = getDistinctServersNumber(messagesReferenceList[msg.SrNb])
		if distinctServersNum >= t+1 {
			var cert = CreateStoreCertif(AckStoreMessagesList)
			cmtInBytes, _ := json.Marshal(cert)
			SendToAllServers(cmtInBytes)
			SendToClient(cmtInBytes, msg.CinitID)
		}
	}
}

func getDistinctServersNumber(m map[string]AckStoreMsg) int {
	var serversList = map[string]string{}
	var i = 0
	for _, message := range m {
		_, exist := serversList[message.SID]
		if !exist {
			i = i + 1
			serversList[message.SID] = message.SID

		}
	}
	return i
}

// fonction d'envoi d'un message à tous les serveurs : elle prend comme paramétre le nom du sujet kafka et le message à envoyer
func SendToAllServers(message []byte) error {
	var topic = "servers"
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
