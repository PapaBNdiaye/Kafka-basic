# Exercice 10 - Détection des records climatiques locaux

## Objectif

Job Spark qui analyse les données historiques HDFS pour chaque ville afin de détecter les records climatiques locaux et les émettre dans Kafka pour exploitation par un dashboard.

## Fonctionnalités

- Analyse des données historiques stockées dans HDFS
- Détection des records climatiques :
  - Jour le plus chaud de la décennie
  - Jour le plus froid de la décennie
  - Rafale de vent la plus forte
  - Jour le plus pluvieux
- Émission des records dans Kafka (topic: `weather_records`)
- Sauvegarde dans HDFS selon la structure `/hdfs-data/{country}/{city}/weather_records`
- Calcul de statistiques générales par ville

## Prérequis

- Python 3.8+
- Spark 3.5+ avec support Kafka
- Kafka en cours d'exécution
- HDFS en cours d'exécution
- Données historiques de l'exercice 9 disponibles dans HDFS

## Installation

```bash
pip install pyspark kafka-python findspark
```

## Utilisation

### Prérequis

**IMPORTANT :** L'exercice 9 doit être exécuté en premier pour générer les données historiques dans HDFS.

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
pip install pyspark kafka-python findspark
```

3. **Configuration des variables d'environnement :**
```bash
# Windows PowerShell
$env:JAVA_HOME = "C:\Program Files\Java\jdk1.8.0_XXX"
$env:HADOOP_HOME = "C:\hadoop"
$env:SPARK_HOME = "C:\spark"

# Linux/Mac
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk
export HADOOP_HOME=/opt/hadoop
export SPARK_HOME=/opt/spark
```

4. **Vérification des services :**
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

# Vérifier que les données de l'exercice 9 existent
hdfs dfs -ls /hdfs-data/France/Paris/weather_history_raw/
```

**Services requis complets :**
- **ZooKeeper** : Port 2181 (requis par Kafka)
- **Kafka** : Port 9092 (stockage des messages)
- **HDFS NameNode** : Port 9870 (métadonnées HDFS)
- **HDFS DataNode** : Port 9864 (données HDFS)
- **YARN ResourceManager** : Port 8088 (gestion des ressources)
- **YARN NodeManager** : Port 8042 (exécution des tâches Spark)

### Exécution du script

**Commande principale :**
```bash
python weather_records_analyzer.py
```

**Exécution avec PowerShell (Windows) :**
```powershell
# Dans le dossier exo10
python .\weather_records_analyzer.py

# Ou via le script PowerShell
.\run_exercice10.ps1
```

**Exécution avec variables d'environnement Hadoop :**
```bash
# S'assurer que Hadoop est dans le PATH
export PATH=$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH
python weather_records_analyzer.py
```

### Vérification des résultats

**1. Vérification Kafka :**
```bash
# Consulter les records dans le topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic weather_records --from-beginning --max-messages 10
```

**2. Vérification HDFS :**
```bash
# Lister les fichiers de records créés
hdfs dfs -ls /hdfs-data/France/Paris/weather_records/

# Voir le contenu d'un fichier de records
hdfs dfs -cat /hdfs-data/France/Paris/weather_records/climate_records_*.json | head -20
```

**3. Interface web HDFS :**
- Ouvrir : http://localhost:9870
- Naviguer vers : /hdfs-data/France/Paris/weather_records/

### Dépannage

**Erreur "Spark session not found" :**
```bash
# Vérifier que Spark est correctement installé
spark-submit --version
```

**Erreur de lecture HDFS :**
```bash
# Vérifier que les données de l'exercice 9 existent
hdfs dfs -ls -R /hdfs-data/

# Si les données n'existent pas, exécuter d'abord l'exercice 9
cd ../exo9
python weather_history_retriever.py
```

**Erreur de connexion Kafka :**
```bash
# Vérifier que Kafka est actif
kafka-topics --bootstrap-server localhost:9092 --list

# Redémarrer Kafka si nécessaire
.\kafka\bin\windows\kafka-server-start.bat .\kafka\config\server.properties
```

**Erreur Java/Hadoop :**
```bash
# Vérifier la configuration Java
java -version
echo $JAVA_HOME

# Vérifier que winutils.exe est présent (Windows)
dir C:\hadoop\bin\winutils.exe
```

## Configuration

### Entrée HDFS

Le script lit les données depuis :
```
/hdfs-data/{country}/{city}/weather_history_raw/*.json
```

### Sortie Kafka

Les records sont envoyés au topic :
```
weather_records
```

### Sortie HDFS

Les records sont sauvegardés dans :
```
/hdfs-data/{country}/{city}/weather_records/climate_records_{timestamp}.json
```

## Structure des données

### Format d'entrée (HDFS)

Les données historiques au format JSON avec la structure suivante :
```json
{
  "city": "Paris",
  "country": "France",
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

### Format de sortie (Kafka/HDFS)

Les records détectés au format JSON :
```json
{
  "city": "Paris",
  "country": "France",
  "record_type": "temperature_max",
  "value": 38.2,
  "date": "2024-07-15",
  "year": 2024,
  "description": "Jour le plus chaud: 38.2°C le 2024-07-15",
  "timestamp": "2024-01-15T10:30:00"
}
```

## Types de records détectés

1. **temperature_max** : Jour avec la température maximale la plus élevée
2. **temperature_min** : Jour avec la température minimale la plus basse
3. **windspeed_max** : Jour avec la rafale de vent la plus forte
4. **precipitation_max** : Jour avec les précipitations les plus importantes
5. **statistics** : Statistiques générales (moyennes, nombre de jours)

## Gestion des erreurs

Le script gère automatiquement :
- Les erreurs de lecture HDFS
- Les erreurs de connexion Kafka
- Les erreurs de traitement Spark
- Les données manquantes ou invalides
- Les interruptions utilisateur (Ctrl+C)

## Logs

Le script génère des logs détaillés incluant :
- Le nombre de fichiers traités depuis HDFS
- Le nombre d'enregistrements explosés
- Le nombre de records calculés par ville
- Les erreurs de traitement
- Les confirmations de sauvegarde

## Performance

- Utilisation de Spark SQL pour l'analyse
- Explosion optimisée des données quotidiennes
- Traitement par ville pour éviter la surcharge mémoire
- Sauvegarde par chunks pour optimiser les écritures HDFS

## Exemple de sortie

```
=== EXERCICE 10: DETECTION DES RECORDS CLIMATIQUES LOCAUX ===
Job Spark analysant HDFS pour détecter les records climatiques
Émission des records dans Kafka et sauvegarde dans HDFS

2024-01-15 10:30:00 - INFO - Chargement des données historiques depuis HDFS...
2024-01-15 10:30:01 - INFO - Données historiques chargées: 50 fichiers
2024-01-15 10:30:02 - INFO - Explosion des données quotidiennes...
2024-01-15 10:30:03 - INFO - Données explosées: 18250 enregistrements
2024-01-15 10:30:04 - INFO - Calcul des records climatiques...
2024-01-15 10:30:05 - INFO - Traitement des records pour Paris, France
2024-01-15 10:30:06 - INFO - Records calculés pour 5 villes
2024-01-15 10:30:07 - INFO - Envoi de 25 records vers Kafka...
2024-01-15 10:30:08 - INFO - Tous les records envoyés vers Kafka
2024-01-15 10:30:09 - INFO - Sauvegarde des records dans HDFS...
2024-01-15 10:30:10 - INFO - Sauvegarde HDFS terminée

=== RESULTATS DE L'ANALYSE DES RECORDS CLIMATIQUES ===

Paris, France:
  - Jour le plus chaud: 38.2°C le 2024-07-15
  - Jour le plus froid: -5.1°C le 2024-01-20
  - Rafale de vent la plus forte: 89.3 km/h le 2024-03-12
  - Jour le plus pluvieux: 45.2 mm le 2024-11-08

Lyon, France:
  - Jour le plus chaud: 36.8°C le 2024-08-03
  - Jour le plus froid: -7.2°C le 2024-02-15
  - Rafale de vent la plus forte: 76.5 km/h le 2024-01-28
  - Jour le plus pluvieux: 52.1 mm le 2024-09-22

Total: 25 records analysés
Données disponibles dans:
- Kafka topic: weather_records
- HDFS: /hdfs-data/{country}/{city}/weather_records/
```
