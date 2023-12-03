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
Serveur ID : server1
Nombre de Serveurs (N) : 6 
2. **Démarrage du serveur :**

   - Ouvrez un nouveau terminal ou un nouvel onglet.

   - Accédez au répertoire "worker" de ce projet.

   - Exécutez la commande suivante pour démarrer le worker :
     
     ```bash
     cd worker
     go run worker.go
     ```
## Clients : On a 2 Types de clients

### 1. Client basé sur la console(Client Léger) (producerClient.go)

Ce type de producteur interagit avec l'utilisateur via la console pour recueillir les informations nécessaires, puis les diffuse à tous les serveurs.

#### Utilisation

- Ouvrez une autre nouvelle fenêtre de terminal ou un autre onglet.

   - Accédez au répertoire "producer" de ce projet.

   - Exécutez la commande suivante pour démarrer le client producteur :
     
     ```bash
     cd producer
     go run producerClient.go

### 2. Client basé sur une API(Producer2.go)

Ce type de producteur est exposé via une API et accepte des données via une requête POST. Le point de terminaison de l'API est `localhost:3001/api/v1/server1`, et le client attend un format JSON spécifique dans le corps de la requête.

#### Point de Terminaison de l'API

- **Méthode :** POST
- **Point de terminaison :** `localhost:3001/api/v1/server1`

#### Utilisation

- Ouvrez une autre nouvelle fenêtre de terminal ou un autre onglet.

   - Accédez au répertoire "producer" de ce projet.

   - Exécutez la commande suivante pour démarrer le client producteur :
     
     ```bash
     cd producer
     go run producer2.go

#### Format du Corps de la Requête

```json
{
  "text": "data dans le message",
  "id": "ID du client",
  "num-inc": "numero d'instance"
}

## FINALEMENT 
4. **Écrivez les messages que vous souhaitez envoyer.**

C'est tout ! Vous avez maintenant exécuté avec succès ce projet Go avec Docker. N'hésitez pas à ajouter d'autres instructions ou informations pertinentes à ce README en fonction des besoins de votre projet.

Bonne utilisation !

