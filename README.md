# Projet Go avec Docker - README

Ce projet est basé sur Go et utilise Docker pour l'exécution. Avant de commencer, assurez-vous d'avoir Go et Docker configurés sur votre ordinateur.

## Prérequis

- Go doit être installé et configuré : [Installer Go](https://golang.org/doc/install)
- Docker doit être installé : [Installer Docker](https://docs.docker.com/get-docker/)
- Docker Compose doit être installé : [Installer Docker Compose](https://docs.docker.com/compose/install/)

## Instructions d'exécution

Suivez ces étapes pour exécuter ce projet :

1. **Construction et exécution des conteneurs Docker :**

   - Ouvrez un terminal.

   - Accédez au répertoire contenant le fichier Docker Compose de ce projet.

   - Exécutez la commande suivante pour construire et démarrer les conteneurs Docker :
     
     ```bash
     docker-compose up --build
     ```

2. **Démarrage du worker :**

   - Ouvrez un nouveau terminal ou un nouvel onglet.

   - Accédez au répertoire "worker" de ce projet.

   - Exécutez la commande suivante pour démarrer le worker :
     
     ```bash
     cd worker
     go run worker.go
     ```

3. **Démarrage du producteur :**

   - Ouvrez une autre nouvelle fenêtre de terminal ou un autre onglet.

   - Accédez au répertoire "producer" de ce projet.

   - Exécutez la commande suivante pour démarrer le client producteur :
     
     ```bash
     cd producer
     go run producerClient.go
     ```

4. **Écrivez les messages que vous souhaitez envoyer.**

C'est tout ! Vous avez maintenant exécuté avec succès ce projet Go avec Docker. N'hésitez pas à ajouter d'autres instructions ou informations pertinentes à ce README en fonction des besoins de votre projet.

Bonne utilisation !
