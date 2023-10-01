package main

import (
	"encoding/json" //librarie utilisée pour le marshal/unmarshal des objets
	"fmt"           // utiliser pour l'affichages des message
	"os"
	"os/signal"
	"reflect" // utilisé pour compater si deux maps sont identiques avec la methode deepEquals
	"syscall"

	// "strings"

	"github.com/Shopify/sarama"
)

var refList = map[string]map[string]Comment{} // la liste des message recu par le serveur et envoyée par des clients , regroupées par numero d'instance , sans redondance
var serverList = map[string]map[string]map[string]string{}
var sent = false // variable pour s'assurer que le serveur diffuse son message à tous les autres serveurs qu'une seule fois

const s = 4 // represente le variable s qui est egale à N/2 :

const n = 6
const serverId = "shared"

type Comment struct {
	//structure des messages envoyées par les clients aux serveurs
	// text : contenu du message num inc : numero d'instance id : Id du message
	Text   string `form:"text" json:"text"`
	Id     string `form:"id" json:"id"`
	NumInc string `form:"num-inc" json:"num-inc"`
}
type MessageSent struct { //structure des messages envoyées par un serveur à tous les autres serveurs
	Text     string            `form:"text" json:"text"`         //contenu du message
	NumInc   string            `form:"num-inc" json:"num-inc"`   // numero d'instance
	IdList   map[string]string `form:"idList" json:"idList"`     // liste des ids maps des ids des message recus [idMessage:idMessage]
	ServerId string            `form:"serverId" json:"serverId"` //id du serveur
}

func main() { // declaration des sujets kafka sur lequelle on va publier/consommer les messages

	topic := "shared"   //sujet specifique à ce serveur sur lequel tous les clients peuvent envoyée des messages
	topic2 := "servers" //sujet pour tous les serveurs
	topic3 := "clients" // sujets pour les message envoyées à partir d'un client à tous les serveurs

	worker, err := connectConsumer([]string{"127.0.0.1:9092"}) //spécifier url du serveur kafka
	if err != nil {
		panic(err)
	}

	// creation des consommateurs pour chaque sujet
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	consumer2, err2 := worker.ConsumePartition(topic2, 0, sarama.OffsetOldest)
	consumer3, err2 := worker.ConsumePartition(topic3, 0, sarama.OffsetOldest)

	if err != nil || err2 != nil {
		panic(err)

	} // test sur les erreurs de démrrage du consommateurs
	fmt.Println("Consumer started ")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// Calculer nombre de messages
	msgCount := 0

	// singaler fin
	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages(): // cas ou le consommtauer recoit un message

				if msg.Value != nil { // si son valeur est different de null
					content := Comment{}
					json.Unmarshal(msg.Value, &content)                                 //transformer en format json
					if content.Text != "" && content.Id != "" && content.NumInc != "" { //si tous les champs sont pas vides on fait appel à la fonction refList
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
func setRefList(msg Comment) { //la fonction prend en parametre le message recu par le serveur à partir d'un client
	if sent == false { // tester si il a déja envoyée un message avant

		_, ok := refList[msg.NumInc]
		if len(refList) == 0 || true { // si la liste est vide donc on va ajouter directement le message dans la liste
			if !ok { // si dans le map il n'existe pas une case avec cet numero d'instance
				refList[msg.NumInc] = make(map[string]Comment) //intialisation du case
			}
			refList[msg.NumInc][msg.Id] = msg //on va ajouter aux map groupées par le numero d'instance le message recu
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

			var msg_sent = MessageSent{}
			msg_sent.IdList = list
			msg_sent.NumInc = msg.NumInc
			msg_sent.Text = msg.Text
			msg_sent.ServerId = serverId

			cmtInBytes, _ := json.Marshal(msg_sent)     // compresser le message avant de l'envoyer
			SendMessageToServers("servers", cmtInBytes) // publier le message dans kafka
			sent = true                                 //changer la valeur du variable à true

		}

	}
} // cet fonction est pour intialiser les paramteres du serveur en tant que producteur des messages
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

// fonction d'envoi d'un message à tous les serveurs : elle prend comme paramétre le nom du sujet kafka et le message à envoyer
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
