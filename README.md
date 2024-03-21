# Projet Go avec Docker - README

Ce projet est basé sur Go et utilise Docker pour l'exécution. Avant de commencer, assurez-vous d'avoir Go et Docker configurés sur votre ordinateur.

## Prérequis

- Go doit être installé et configuré : [Installer Go](https://golang.org/doc/install) go version go1.20.5 windows/amd64
- Docker doit être installé : [Installer Docker](https://docs.docker.com/get-docker/)
- Docker Compose doit être installé : [Installer Docker Compose](https://docs.docker.com/compose/install/)
## Configuration
Le serveur Kafka utilisé par ce projet est configuré avec l'URL suivante : 127.0.0.1:9092
Ce projet utilise la bibliothèque Sarama pour interagir avec Kafka.

## Instructions d'exécution

Suivez ces étapes pour exécuter ce projet :

1. **Construction et exécution des conteneurs Docker :**

   - Ouvrez un terminal.

   - Accédez au répertoire contenant le fichier Docker Compose de ce projet.

   - Exécutez la commande suivante pour construire et démarrer les conteneurs Docker :
     
     ```bash
     docker-compose up --build
     ```

## Serveurs : Fichier Pour lancer le Serveurs.
Le serveur est configuré avec les paramètres suivants :
Serveur ID qui represente l'ID unique du serveur   : server1

Dans notre exemple de code, nous avons utilisé une valeur de 6 pour représenter le nombre total de serveurs (N) .

2. **Démarrage du serveur :**

   - Ouvrez un nouveau terminal ou un nouvel onglet.

   - Accédez au répertoire "worker" de ce projet.

   - Exécutez la commande suivante pour démarrer le worker :
     
     ```bash
     cd worker
     go run worker.go
     ```
## Clients : On a 1 Type de clients

### 1. Client basé sur la console(Client Léger) (producerClient.go)

Ce type de producteur interagit avec l'utilisateur via la console pour recueillir les informations nécessaires, puis les diffuse à tous les serveurs.

#### Utilisation

- Ouvrez une autre nouvelle fenêtre de terminal ou un autre onglet.

   - Accédez au répertoire "producer" de ce projet.

   - Exécutez la commande suivante pour démarrer le client producteur :
     
     ```bash
     cd producer
     go run producerClient.go

## Messages : On a principalement 2 Types de message échangés
### 1. StoreReqMsg
Le type de message envoyé par un client à tous les serveurs , destiné à être reçu par tous les serveurs.
Ce type message contient les champs suivants :
#### Data : le contenu utile est une chaîne de caractères quelconque.
#### CID : numéro d’instance chiffre (c’est l’ID du client expéditeur)
#### SrNb : Un chiffre arbitraire et distinct est associé à chaque StoreReq
#### HighPriority : Un drapeau représentant la priorité du message est réglé sur "true" s'il est important, et sur "false" sinon.
#### SigC : Signature du client
### 2. AckStoreMsg
Le type de message créé par un serveur lorsqu'il reçoit un StoreReqMsg.
Ce type message contient les champs suivants :
#### Data : le contenu utile est une chaîne de caractères quelconque.
#### CinitID : numéro d’instance chiffre (c’est l’ID du client initiateur)
#### SrNb : Un chiffre arbitraire et distinct est associé à chaque StoreReq
#### IdList : liste des ids messages recus.
#### SID : L'ID du serveur expéditeur.
#### SigCinit : signature du client initiateur = 0 (pour l’instant).
#### SigServ : signature du serveur expéditeur  = 0 (pour l’instant).
## Méthodes :
### 1. initializeSenderForMessages
Cette fonction est utilisée pour configurer les paramètres de Kafka afin de spécifier le serveur à utiliser comme envoyeur de messages, ainsi que d'autres paramètres de configuration nécessaires pour l'envoi efficace de messages via Kafka.
### 2. initializeListenerForMessages
Cette fonction est utilisée pour configurer les paramètres de Kafka afin de spécifier le serveur à utiliser comme récepteur de messages, ainsi que d'autres paramètres de configuration nécessaires pour la réception efficace de messages via Kafka.
### 3. initializekafkaServer
une fonction qui permet d'initialiser les paramètres du serveur Kafka, tels que l'URL du courtier Kafka et les sujets (topics).
### 4. handleRecieveMessages
Une fonction utilisé pour écouter et gérer les différents messages reçus, que ce soit ceux du client vers le serveur ou entre serveurs.
C'est tout ! Vous avez maintenant exécuté avec succès ce projet Go avec Docker. N'hésitez pas à ajouter d'autres instructions ou informations pertinentes à ce README en fonction des besoins de votre projet.
### 5. SendToClient
envoie un message kafka au client n° cID
### 6.SendToAllServers
envoie un message kakfka à tous les serveurs
### 7.CreateStoreCertif
en gros cette fonction vérifie que tous les messages ont le même data, puis output la concaténation des sigServ
### 8.checkForMsgId
Cette fonction sert à vérifier si l'identifiant du message reçu existe déjà ou non.
### 9.HandleStoreReqMsg
fonction exécutée par un serveur lorsqu’il reçoit un message de type StoreReqMsg 
### 10.getDistinctServersNumber 
Fonction renvoyant le nombre de serveurs différents à partir desquels des messages ont été reçus.
### 11.HandleAckStoreMsg
Lorsqu'un serveur reçoit un message AckStoreMsg, cette fonction est exécutée pour traiter les messages

Les détails supplémentaires sur les fonctions sont inclus dans le code lui-même.
Bonne utilisation !

