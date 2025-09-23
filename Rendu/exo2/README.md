# Exercice 2 : Consommateur Kafka

## Objectif
Cet exercice consiste à créer un consommateur Python qui lit les messages depuis un topic Kafka et les affiche en temps réel.

## Instructions d'exécution

### Prérequis
1. **Services Kafka** : ZooKeeper et Kafka Server doivent être démarrés
2. **Environnement Python** : Virtual environment activé avec les dépendances installées

### Commandes de démarrage des services
```bash
# Terminal 1 - ZooKeeper
start-zookeeper.bat

# Terminal 2 - Kafka Server  
start-kafka.bat
```

### Activation de l'environnement Python
```bash
cd TP\solution\exo2
venv\Scripts\Activate.ps1
```

### Exécution du consommateur
```bash
python consumer.py <nom_du_topic>
```

**Exemples :**
```bash
# Consommer le topic weather_stream
python consumer.py weather_stream

# Consommer un autre topic
python consumer.py mon_topic
```

## Fonctionnalités du script

- **Lecture en temps réel** : Affiche les messages dès leur arrivée
- **Parsing JSON** : Tente de parser les messages JSON avec fallback texte
- **Informations détaillées** : Affiche partition, offset, timestamp
- **Gestion des erreurs** : Messages d'erreur clairs si Kafka n'est pas disponible
- **Arrêt propre** : Ctrl+C pour arrêter, fermeture propre du consommateur

## Résultat attendu
Le script affiche chaque message reçu avec :
- Timestamp de réception
- Numéro de message
- Partition et offset
- Contenu du message (JSON formaté ou texte brut)

Le consommateur reste actif jusqu'à interruption manuelle.