# Exercice 9 - Récupération de séries historiques longues

## Objectif

Télécharger des données météo sur **10 ans** pour une ville donnée (ex. Paris) via l'API archive Open-Meteo et stocker les données brutes dans Kafka et HDFS.

## Fonctionnalités

- Récupération de données météorologiques historiques via l'API archive Open-Meteo
- Support de 5 villes européennes (Paris, Lyon, Berlin, Madrid, Rome)
- Stockage des données dans Kafka (topic: `weather_history_raw`)
- Sauvegarde dans HDFS selon la structure `/hdfs-data/{country}/{city}/weather_history_raw`
- Mode test disponible pour récupérer seulement 1 an de données

## Données récupérées

Pour chaque jour sur la période sélectionnée :
- Température maximale et minimale
- Précipitations
- Vitesse maximale du vent
- Code météo

## Prérequis

- Python 3.8+
- Zookeeper en cours d'execution
- Kafka en cours d'exécution
- HDFS en cours d'exécution
- Accès internet pour l'API Open-Meteo

## Installation

```bash
pip install requests kafka-python hdfs
```

## Utilisation

### Préparation de l'environnement

1. **Activation de l'environnement virtuel :**
```bash
# Windows PowerShell
.\venv\Scripts\Activate.ps1

# Linux/Mac
source venv/bin/activate
```

2. **Installation des dépendances :**
```bash
pip install -r requirements.txt
```

3. **Vérification des services :**
```bash
# Vérifier que ZooKeeper est actif (port 2181)
netstat -an | findstr 2181

# Vérifier que Kafka est actif (port 9092)
netstat -an | findstr 9092

# Vérifier que HDFS NameNode est actif (port 9870)
netstat -an | findstr 9870

# Vérifier que HDFS DataNode est actif (port 9864)
netstat -an | findstr 9864

# Vérifier que YARN ResourceManager est actif (port 8088)
netstat -an | findstr 8088

# Vérifier que YARN NodeManager est actif (port 8042)
netstat -an | findstr 8042
```

**Services requis complets :**
- **ZooKeeper** : Port 2181 (requis par Kafka)
- **Kafka** : Port 9092 (stockage des messages)
- **HDFS NameNode** : Port 9870 (métadonnées HDFS)
- **HDFS DataNode** : Port 9864 (données HDFS)
- **YARN ResourceManager** : Port 8088 (gestion des ressources)
- **YARN NodeManager** : Port 8042 (exécution des tâches)

### Exécution du script

**Commande principale :**
```bash
python weather_history_retriever.py
```

**Exécution avec PowerShell (Windows) :**
```powershell
# Dans le dossier exo9
python .\weather_history_retriever.py

# Ou via le script PowerShell
.\run_exercice9.ps1
```

### Vérification des résultats

**1. Vérification Kafka :**
```bash
# Consulter les messages dans le topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic weather_history_raw --from-beginning --max-messages 5
```

**2. Vérification HDFS :**
```bash
# Lister les fichiers créés
hdfs dfs -ls /hdfs-data/France/Paris/weather_history_raw/

# Voir le contenu d'un fichier
hdfs dfs -cat /hdfs-data/France/Paris/weather_history_raw/weather_history_2024.json | head -20
```

**3. Interface web HDFS :**
- Ouvrir : http://localhost:9870
- Naviguer vers : /hdfs-data/France/Paris/weather_history_raw/

### Dépannage

**Erreur de connexion Kafka :**
```bash
# Redémarrer Kafka si nécessaire
.\kafka\bin\windows\kafka-server-start.bat .\kafka\config\server.properties
```

**Erreur de connexion HDFS :**
```bash
# Redémarrer HDFS si nécessaire
hdfs namenode -format
start-dfs.cmd
```

## Configuration

### Villes supportées

Le script supporte les villes suivantes avec leurs coordonnées :
- Paris (France)
- Lyon (France) 
- Berlin (Allemagne)
- Madrid (Espagne)
- Rome (Italie)

### Mode test

Pour tester avec seulement 1 an de données, modifier la variable `test_mode = True` dans la fonction `main()`.

## Structure des données

### Format Kafka

Les données sont envoyées au topic `weather_history_raw` au format JSON avec les métadonnées suivantes :

```json
{
  "city": "Paris",
  "country": "France", 
  "retrieval_date": "2024-01-15T10:30:00",
  "period_start": "2024-01-01",
  "period_end": "2024-12-31",
  "year": 2024,
  "daily": {
    "time": ["2024-01-01", "2024-01-02", ...],
    "temperature_2m_max": [8.2, 10.1, ...],
    "temperature_2m_min": [2.1, 4.3, ...],
    "precipitation_sum": [0.0, 1.2, ...],
    "windspeed_10m_max": [12.5, 8.9, ...],
    "weathercode": [3, 61, ...]
  }
}
```

### Structure HDFS

Les fichiers sont sauvegardés dans HDFS selon l'organisation suivante :

```
/hdfs-data/
├── France/
│   ├── Paris/
│   │   └── weather_history_raw/
│   │       ├── weather_history_2024.json
│   │       ├── weather_history_2023.json
│   │       └── ...
│   └── Lyon/
│       └── weather_history_raw/
└── Germany/
    └── Berlin/
        └── weather_history_raw/
```

## Gestion des erreurs

Le script gère automatiquement :
- Les erreurs de connexion à l'API Open-Meteo
- Les timeouts de requête
- Les erreurs de connexion Kafka
- Les erreurs d'écriture HDFS
- Les interruptions utilisateur (Ctrl+C)

## Logs

Le script génère des logs détaillés incluant :
- Le nombre de jours récupérés par année
- Les erreurs de requête API
- Les confirmations de sauvegarde
- Le statut final de la récupération

## Performance

- Pause de 1 seconde entre chaque requête API pour éviter la surcharge
- Timeout de 30 secondes pour les requêtes API
- Retry automatique pour les erreurs Kafka
- Gestion des répertoires HDFS existants

## Exemple de sortie

```
=== EXERCICE 9: RECUPERATION DE SERIES HISTORIQUES LONGUES ===
Téléchargement de données météo sur 10 ans via l'API archive Open-Meteo
Stockage dans Kafka et HDFS

Mode test: Désactivé (10 ans)
Ville sélectionnée: Paris

2024-01-15 10:30:00 - INFO - Début de la récupération des données pour Paris
2024-01-15 10:30:01 - INFO - Récupération des données pour 2024
2024-01-15 10:30:02 - INFO - Données récupérées pour 2024: 366 jours
2024-01-15 10:30:03 - INFO - Données sauvegardées dans HDFS: /hdfs-data/France/Paris/weather_history_raw/weather_history_2024.json
...
Récupération des données terminée avec succès
Données disponibles dans:
- Kafka topic: weather_history_raw
- HDFS: /hdfs-data/France/Paris/weather_history_raw/
```
