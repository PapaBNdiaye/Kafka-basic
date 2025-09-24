# Exercice 13 - Détection d'anomalies climatiques (Batch vs Speed)

## Objectif

Streaming Spark qui lit les données météo en temps réel depuis Kafka, les joint avec les profils saisonniers enrichis stockés dans HDFS, et détecte les anomalies climatiques en comparant les valeurs observées avec les références historiques.

## Fonctionnalités

- **Ingestion temps réel** : Lecture depuis Kafka topic `weather_transformed`
- **Jointure Batch vs Speed** : Association des données temps réel avec les profils historiques
- **Détection d'anomalies** : Comparaison avec des seuils définis
- **Publication temps réel** : Émission des anomalies dans Kafka et sauvegarde HDFS
- **Streaming continu** : Traitement en temps réel avec Spark Streaming

## Prérequis

- Python 3.8+
- Spark 3.5+ avec support Kafka Streaming
- Kafka en cours d'exécution
- HDFS en cours d'exécution
- Profils saisonniers enrichis de l'exercice 12 disponibles dans HDFS

## Installation

```bash
pip install pyspark kafka-python findspark
```

## Utilisation

### Prérequis

**IMPORTANT :** Les exercices 9 et 12 doivent être exécutés en premier :
- Exercice 9 : Données historiques dans HDFS
- Exercice 12 : Profils saisonniers enrichis dans HDFS

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

# Vérifier que les profils enrichis existent
hdfs dfs -ls /hdfs-data/France/Paris/seasonal_profile_enriched/2024/

# Vérifier que le topic d'entrée existe
kafka-topics --bootstrap-server localhost:9092 --list | grep weather_transformed
```

**Services requis complets :**
- **ZooKeeper** : Port 2181 (requis par Kafka)
- **Kafka** : Port 9092 (streaming des données)
- **HDFS NameNode** : Port 9870 (métadonnées HDFS)
- **HDFS DataNode** : Port 9864 (données HDFS)
- **YARN ResourceManager** : Port 8088 (gestion des ressources)
- **YARN NodeManager** : Port 8042 (exécution des tâches Spark Streaming)

### Préparation des données d'entrée

**IMPORTANT :** Le script lit depuis le topic `weather_transformed`. Si ce topic n'existe pas ou est vide :

1. **Créer le topic si nécessaire :**
```bash
kafka-topics --bootstrap-server localhost:9092 --create --topic weather_transformed --partitions 3 --replication-factor 1
```

2. **Générer des données de test (optionnel) :**
```bash
# Utiliser le producer de l'exercice 6 ou créer un producer de test
python generate_test_data.py
```

### Exécution du script

**Commande principale :**
```bash
python anomaly_detector.py
```

**Exécution avec PowerShell (Windows) :**
```powershell
# Dans le dossier exo13
python .\anomaly_detector.py

# Ou via le script PowerShell
.\run_exercice13.ps1
```

**Exécution avec variables d'environnement Hadoop :**
```bash
# S'assurer que Hadoop est dans le PATH
export PATH=$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH
python anomaly_detector.py
```

**Le script fonctionne en continu :**
- Appuyez sur `Ctrl+C` pour l'arrêter
- Il traite les messages au fur et à mesure qu'ils arrivent
- Les anomalies sont détectées et publiées en temps réel

### Vérification des résultats

**1. Vérification Kafka (anomalies) :**
```bash
# Consulter les anomalies détectées (dans un autre terminal)
kafka-console-consumer --bootstrap-server localhost:9092 --topic weather_anomalies --from-beginning --max-messages 10
```

**2. Vérification HDFS :**
```bash
# Lister les fichiers d'anomalies créés
hdfs dfs -ls /hdfs-data/France/Paris/anomalies/2024/1/

# Voir le contenu d'un fichier d'anomalies
hdfs dfs -cat /hdfs-data/France/Paris/anomalies/2024/1/anomalies_*.json | head -20
```

**3. Interface web HDFS :**
- Ouvrir : http://localhost:9870
- Naviguer vers : /hdfs-data/France/Paris/anomalies/2024/1/

**4. Surveillance en temps réel :**
```bash
# Surveiller les logs du script (dans un autre terminal)
tail -f anomaly_detector.log

# Ou surveiller les messages Kafka
kafka-console-consumer --bootstrap-server localhost:9092 --topic weather_anomalies
```

### Dépannage

**Erreur "Spark session not found" :**
```bash
# Vérifier que Spark est correctement installé
spark-submit --version

# Vérifier les variables d'environnement
echo $SPARK_HOME
echo $JAVA_HOME
```

**Erreur "Topic not found" :**
```bash
# Créer le topic d'entrée
kafka-topics --bootstrap-server localhost:9092 --create --topic weather_transformed --partitions 3 --replication-factor 1

# Créer le topic de sortie
kafka-topics --bootstrap-server localhost:9092 --create --topic weather_anomalies --partitions 3 --replication-factor 1
```

**Erreur "No data in input topic" :**
```bash
# Vérifier que le topic contient des données
kafka-console-consumer --bootstrap-server localhost:9092 --topic weather_transformed --from-beginning --max-messages 1

# Si vide, générer des données de test ou utiliser l'exercice 6
```

**Erreur "Profiles not found" :**
```bash
# Vérifier que les profils enrichis existent
hdfs dfs -ls -R /hdfs-data/*/*/seasonal_profile_enriched/

# Si manquants, exécuter d'abord l'exercice 12
cd ../exo12
python profiles_validator_enricher.py
```

**Erreur de streaming :**
```bash
# Vérifier les checkpoints Spark
ls -la /tmp/anomaly-detector-checkpoint/

# Nettoyer les checkpoints si nécessaire
rm -rf /tmp/anomaly-detector-checkpoint/
```

**Erreur Java/Hadoop :**
```bash
# Vérifier la configuration Java
java -version

# Vérifier que winutils.exe est présent (Windows)
dir C:\hadoop\bin\winutils.exe

# Vérifier les permissions HDFS
hdfs dfs -ls /
```

## Configuration

### Entrée Kafka

Le script lit les données temps réel depuis :
```
weather_transformed
```

### Entrée HDFS

Les profils de référence sont chargés depuis :
```
/hdfs-data/{country}/{city}/seasonal_profile_enriched/{year}/profile.json
```

### Sortie Kafka

Les anomalies détectées sont envoyées vers :
```
weather_anomalies
```

### Sortie HDFS

Les anomalies sont sauvegardées dans :
```
/hdfs-data/{country}/{city}/anomalies/{year}/{month}/anomalies_{timestamp}.json
```

## Seuils d'anomalie

### Température
- **Seuil** : |température_observée - temp_mean| > 5°C
- **Référence** : temp_mean du profil saisonnier

### Vent
- **Seuil** : windspeed_observé > wind_mean + (2 × wind_std)
- **Référence** : wind_mean et wind_std du profil saisonnier

### Alertes météo
- **Seuil** : écart significatif entre alerte observée et probabilité historique
- **Définition d'écart significatif** : différence > 30%
- **Référence** : alert_probability_level_1 et alert_probability_level_2

## Structure des données

### Format d'entrée (Kafka)

```json
{
  "city": "Paris",
  "country": "France",
  "timestamp": "2024-01-15T10:30:00Z",
  "temperature": 25.3,
  "windspeed": 15.2,
  "precipitation": 0.5,
  "weathercode": 61,
  "alert_level": 1
}
```

### Format de sortie (Kafka/HDFS)

```json
{
  "event_time": "2024-01-15T10:30:00Z",
  "city": "Paris",
  "country": "France",
  "month": 1,
  "is_anomaly": true,
  "anomaly_count": 2,
  "anomaly_types": ["temperature", "wind"],
  "anomaly_details": [
    {
      "type": "temperature",
      "details": {
        "observed_value": 25.3,
        "expected_value": 4.1,
        "deviation": 21.2,
        "threshold": 5.0
      }
    },
    {
      "type": "wind",
      "details": {
        "observed_value": 15.2,
        "expected_value": 12.3,
        "threshold": 16.5,
        "deviation": 2.9
      }
    }
  ],
  "original_data": {
    "temperature": 25.3,
    "windspeed": 15.2,
    "precipitation": 0.5,
    "weathercode": 61,
    "alert_level": 1
  },
  "reference_profile": {
    "temp_mean": 4.1,
    "wind_mean": 12.3,
    "wind_std": 2.1,
    "alert_prob_level_1": 0.15,
    "alert_prob_level_2": 0.05
  },
  "detection_timestamp": "2024-01-15T10:30:01.123456"
}
```

## Types d'anomalies détectées

1. **temperature** : Écart de température significatif par rapport à la moyenne historique
2. **wind** : Vitesse de vent anormalement élevée par rapport aux statistiques historiques
3. **alert** : Écart significatif entre les alertes observées et les probabilités historiques

## Gestion des erreurs

Le script gère automatiquement :
- Les erreurs de connexion Kafka
- Les erreurs de lecture HDFS
- Les erreurs de traitement Spark Streaming
- Les données manquantes ou invalides
- Les interruptions utilisateur (Ctrl+C)
- La gestion du cache des profils saisonniers

## Logs

Le script génère des logs détaillés incluant :
- Le chargement des profils saisonniers depuis HDFS
- Le démarrage du streaming Kafka
- Le traitement des messages temps réel
- La détection des anomalies
- Les erreurs de traitement
- Les confirmations de publication

## Performance

- Utilisation de Spark Streaming pour le traitement temps réel
- Cache des profils saisonniers pour optimiser les jointures
- Traitement par batch pour optimiser les performances
- Checkpointing pour la récupération après arrêt

## Exemple de sortie

```
=== EXERCICE 13: DETECTION D'ANOMALIES CLIMATIQUES ===
Streaming Spark pour détecter les anomalies en temps réel
Jointure Batch vs Speed avec les profils saisonniers enrichis

2024-01-15 10:30:00 - INFO - Chargement des profils saisonniers enrichis depuis HDFS...
2024-01-15 10:30:01 - INFO - Profils saisonniers chargés pour 5 villes
2024-01-15 10:30:02 - INFO - Démarrage du streaming pour la détection d'anomalies...
2024-01-15 10:30:03 - INFO - Streaming démarré - Détection d'anomalies en cours...
2024-01-15 10:30:03 - INFO - Appuyez sur Ctrl+C pour arrêter
2024-01-15 10:30:10 - INFO - Traitement de 5 messages temps réel
2024-01-15 10:30:10 - INFO - 2 anomalies détectées et traitées
2024-01-15 10:30:11 - INFO - Anomalies envoyées vers Kafka topic: weather_anomalies
2024-01-15 10:30:12 - INFO - Anomalies sauvegardées: /hdfs-data/France/Paris/anomalies/2024/1/anomalies_20240115_103012.json
2024-01-15 10:30:15 - INFO - Traitement de 3 messages temps réel
2024-01-15 10:30:15 - INFO - 1 anomalies détectées et traitées
```

## Surveillance

Le script fonctionne en continu et peut être surveillé via :
- Les logs détaillés en temps réel
- Le topic Kafka `weather_anomalies` pour les anomalies détectées
- Les fichiers HDFS dans `/hdfs-data/{country}/{city}/anomalies/`
- L'interface web HDFS pour vérifier les sauvegardes
