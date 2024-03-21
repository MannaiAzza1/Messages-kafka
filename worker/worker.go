package main

import (
	"encoding/json" //librarie utilisée pour le marshal/unmarshal des objets
	"fmt"           // utiliser pour l'affichages des message
	"os"
	// "strings"

	"github.com/Shopify/sarama"
)

// structure StoreCertif
type Cert struct {
	QuorumSigs map[string]string // liste à un élément: la paire (message.sID , message.sigServ )
	CinitID1   string            // Id du premier client initiateur
	SrNb       string            // numéro d’instance = chiffre arbitraire
}
type StoreReqMsg struct { //type de message envoyé par un client à tous les serveurs
	Data         string `form:"text" json:"text"`                   //contenu utile chaîne de caractères arbitraire
	CID          string `form:"id" json:"id"`                       //  numéro d’instance chiffre entre 1 et L (c’est l’ID du client expéditeur)
	SrNb         string `form:"num-inc" json:"num-inc"`             //chiffre arbitraire, distinct pour chaque StoreReq
	HighPriority bool   `form:"high_priority" json:"high_priority"` // flag  false par défaut, true sinon
	SigC         string `form:"sigc" json:"sigc"`                   // signature du client
}
type AckStoreMsg struct { //type de message créé par un serveur lorsqu’il reçoit un StoreReqMsg
	Data     string            `form:"text" json:"text"`
	SrNb     string            `form:"num-inc" json:"num-inc"`   // numéro d’instance = chiffre arbitraire
	CinitID  string            `form:"cinitID" json:"cinitID"`   //chiffre entre 1 et L (c’est l’ID du client initiateur)
	IdList   map[string]string `form:"idList" json:"idList"`     // liste des id messages recus
	SID      string            `form:"serverId" json:"serverId"` // id su serveur chiffre entre 1 et N (c’est l’ID du serveur expéditeur)
	SigCinit string            `form:"sigCinit" json:"sigCinit"` //signature du client initiateur = 0 (pour l’instant)
	SigServ  string            `form:"sigServ" json:"sigServ"`   // signature de l’expéditeur  = 0 (pour l’instant)

}

var messagesReferenceList = map[string]map[string]AckStoreMsg{} // la liste des message recu par le serveur et envoyée par des clients , regroupées par numero d'instance , sans redondance
var serversList = map[string]map[string]map[string]string{}     /// la liste des serveurs
const UrlKafka = "127.0.0.1:9092"                               // l'url du serveur kafka
const t = 2
const n = t*2 + 1          //N serveurs
const s = 2                // represente le variable s qui est egale à N/2 :
const serverId = "server1" // ID du serveur

var AckStoreMessagesList = map[string]map[int]AckStoreMsg{}
var storeResMessagesList = map[string]map[int]StoreReqMsg{}
var topicsClients = map[string]string{} //List des correspondence sujet sur kafka/ID client pour les clients
var message = make(map[int]AckStoreMsg) // Initalisation du message

// fonction pour intialiser les paramteres du serveur en tant que producteur des messages(envoyer des messages)
func initializeSenderForMessages(brokersUrl []string) (sarama.SyncProducer, error) {
	// configuration kafka( nombre d'essai, acquittement ...)
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

// fonction pour intialiser les paramteres du serveur en tant que consommateur des messages(ecouter des messages recus)
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

// fonction pour intialiser les parametres du serveur kafka (url kafka broker/ topics ...)
func initializekafkaServer() {
	toServer := serverId            //sujet specifique à ce serveur sur lequel tous les clients peuvent envoyée des messages
	serverToAllServers := "servers" //sujet pour tous les serveurs
	clientToAllServers := "clients" // sujets pour les message envoyées à partir d'un client à tous les serveurs

	worker, err := initializeListenerForMessages([]string{UrlKafka}) //spécifier url du serveur kafka
	if err != nil {
		panic(err)
	}

	toServerConsumer, err := worker.ConsumePartition(toServer, 0, sarama.OffsetOldest)
	serverToAllServersConsumer, err := worker.ConsumePartition(serverToAllServers, 0, sarama.OffsetOldest)
	clientToAllServersConsumer, err := worker.ConsumePartition(clientToAllServers, 0, sarama.OffsetOldest)

	if err != nil {
		panic(err)

	}
	fmt.Println("Consumer started ")
	handleRecieveMessages(toServerConsumer, serverToAllServersConsumer, clientToAllServersConsumer, worker)
}

// fonction pour ecouter et gérer les differents messages recu (client to server/ server to server)
func handleRecieveMessages(toServerConsumer sarama.PartitionConsumer, serverToAllServersConsumer sarama.PartitionConsumer, clientToAllServersConsumer sarama.PartitionConsumer, worker sarama.Consumer) {
	sigchan := make(chan os.Signal, 1)

	// singaler fin
	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-toServerConsumer.Errors():
				fmt.Println(err)
			case receivedMessageToServer := <-toServerConsumer.Messages(): // cas ou le serveur recoit un message destinée qu'a lui à partir d'un client

				if receivedMessageToServer.Value != nil { // si son valeur est different de null
					content := StoreReqMsg{}
					json.Unmarshal(receivedMessageToServer.Value, &content)            //transformer en format json
					if content.Data != "" && content.CID != "" && content.SrNb != "" { //si tous les champs sont pas vides on fait appel à la fonction messagesReferenceList
						HandleStoreReqMsg(content)
						fmt.Printf("Received message specific to this server:  Topic(%s) | Message(%s) \n", string(receivedMessageToServer.Topic), string(receivedMessageToServer.Value))

					}

				}

			case receivedMessage := <-serverToAllServersConsumer.Messages(): // si le serveur recoit des message à partir d'un autre serveur qui est destinée pour tous les serveurs
				bytes := []byte(string(receivedMessage.Value))
				var message AckStoreMsg
				fmt.Printf("Received message specific to all servers:  Topic(%s) | Message(%s) \n", string(receivedMessage.Topic), string(receivedMessage.Value))
				json.Unmarshal(bytes, &message)
				HandleAckStoreMsg(message) //fait appel a la methode setServerList

			case receivedMessageFromClient := <-clientToAllServersConsumer.Messages(): // le serveur recoit un message envoyé par un client et destinée à tous les serveurs
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

// envoie msg au client n° cID
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

// envoie msg à tous les serveurs
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
func CreateStoreCertif(list map[string]map[int]AckStoreMsg) Cert { // utilisé par un serveur lorsqu’il a reçu t+1  AckStoreMsg du même ID :
	// en gros cette fonction vérifie que tous les messages ont le même data, puis output la concaténation des sigServ
	var i = 1
	var output Cert
	var messages = map[int]AckStoreMsg{}
	var quorumSigs = map[string]string{}
	var amsg = AckStoreMsg{}

	for _, server := range list { // Préparation de la liste des messages
		for _, message := range server {
			messages[i] = message
			i = i + 1
		}
		amsg = messages[1]
		for _, message := range messages {
			if message.Data == amsg.Data { //   assert amsg.data = data1 % sinon, renvoie une exception “client cinitID1 corrompu”
				quorumSigs[message.SID] = message.SigServ

			} else {
				panic("client cinitID1 corrompu")

			}
		}

	}
	output.QuorumSigs = quorumSigs
	output.CinitID1 = amsg.CinitID
	output.SrNb = amsg.SrNb
	return output //output (cinitID1, srNb, quorumSigs )
}

// sert à verifier si id msg recu exite déja ou non
func checkForMsgId(msg StoreReqMsg, m map[int]StoreReqMsg) bool {
	var isPresent = false
	for _, message := range m {
		if msg.CID == message.CID {
			isPresent = true
		}

	}
	return isPresent
}

// fonction exécutée par un serveur lorsqu’il reçoit un StoreReqMsg msg
func HandleStoreReqMsg(msg StoreReqMsg) {
	_, exist := storeResMessagesList[msg.SrNb]
	if !exist || (checkForMsgId(msg, storeResMessagesList[msg.SrNb]) == false) { // vérifier si c’est c’est la première fois qu’il reçoit un StoreReqMsg avec ce msg.cID et ce msg.srNb
		if msg.HighPriority == true {
			var msgConvert = AckStoreMsg{SID: serverId, SrNb: msg.SrNb, CinitID: msg.CID, Data: msg.Data}
			cmtInBytes, _ := json.Marshal(msgConvert)
			SendToAllServers(cmtInBytes)
		}
		var asmsg = AckStoreMsg{SID: serverId, SrNb: msg.SrNb, CinitID: msg.CID, Data: msg.Data}
		asmsgInBytes, _ := json.Marshal(asmsg)
		SendToAllServers(asmsgInBytes)
		SendToClient(asmsgInBytes, msg.CID)

	}
	var message = make(map[int]StoreReqMsg)
	message[len(storeResMessagesList[msg.SrNb])+1] = msg
	storeResMessagesList[msg.SrNb] = message //stocker le message recu dans la liste

}

// fonctin pour retourner le nombre de serveur distincts
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

// fonction exécutée par un serveur lorsqu’il reçoit un AckStoreMsg
func HandleAckStoreMsg(msg AckStoreMsg) {
	serversList[msg.SID] = make(map[string]map[string]string)
	message[len(AckStoreMessagesList[msg.SID])+1] = msg
	AckStoreMessagesList[msg.SID] = message //stocker msg %plus efficacement: ajouter msg.sID à une liste L égale à l’entrée d’un dictionnaire dict avec la clé (cinitID, srNb)

	if len(AckStoreMessagesList[msg.SID]) > t+1 {
		CreateStoreCertif(AckStoreMessagesList)

	}

	if msg.SID != "" {
		_, ok := messagesReferenceList[msg.SrNb]
		if len(messagesReferenceList) == 0 || true { //if <tester s’il a reçu t+1 AckStoreMsg au total , pour le même client initiateur cinitID et le même srNb,
			// en provenance de t+1 serveurs distincts, c’est à dire t+1 sID distincts
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

// fonction main
func main() {
	topicsClients["client1"] = "client1"
	initializekafkaServer()

}
