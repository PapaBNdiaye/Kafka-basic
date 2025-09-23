# Exercice 1 - Mise en place de Kafka

## Objectif
Créer un topic Kafka nommé `weather_stream` et envoyer un message statique.

## Fichier à exécuter
`exercice1_rendu.ps1`

## Commande d'exécution
```powershell
powershell -ExecutionPolicy Bypass -File "exercice1_rendu.ps1"
```

## Ce que fait le script
1. Crée le topic `weather_stream` avec 1 partition
2. Vérifie que le topic existe dans la liste
3. Envoie le message JSON `{"msg": "Hello Kafka"}`
4. Lit les messages pour vérifier la réception

## Résultat attendu
Le script affiche la création du topic et les messages reçus par le consommateur.

## Prérequis
- Kafka et ZooKeeper démarrés
- PowerShell avec droits d'exécution

## Notes
Le script peut afficher un avertissement si le topic existe déjà, c'est normal.