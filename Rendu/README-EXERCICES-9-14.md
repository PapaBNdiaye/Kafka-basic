# Exercices 9-14 - Système de Météo Avancé

## Vue d'ensemble

Ce dossier contient les exercices 9-14 qui construisent un système complet de météo avancé avec :
- Récupération de données historiques longues (10 ans)
- Analyse des records climatiques
- Profils saisonniers et validation statistique
- Détection d'anomalies en temps réel
- Interface web de visualisation

## Architecture du système

```
Données Sources (API Open-Meteo)
    ↓
Exercice 9: Récupération historique (Kafka + HDFS)
    ↓
Exercice 10: Records climatiques (Spark → Kafka + HDFS)
    ↓
Exercice 11: Profils saisonniers (Spark → HDFS)
    ↓
Exercice 12: Validation & enrichissement (Spark → HDFS)
    ↓
Exercice 13: Détection d'anomalies (Spark Streaming → Kafka + HDFS)
    ↓
Exercice 14: Interface web (Flask + Kafka + HDFS)
```

## Prérequis

### Services requis
- **ZooKeeper** (port 2181) - Coordination Kafka
- **Kafka** (port 9092) - Messaging
- **HDFS NameNode** (port 9870) - Métadonnées
- **HDFS DataNode** (port 9864) - Stockage
- **YARN ResourceManager** (port 8088) - Gestion ressources
- **YARN NodeManager** (port 8042) - Exécution tâches

### Logiciels
- Python 3.8+
- Java 8 (OpenJDK)
- Hadoop 3.3.6
- Spark 3.5+
- Kafka 3.9.1

### Dépendances Python
```bash
pip install -r requirements.txt
```

## Installation et configuration

### 1. Vérification des services
```bash
# Vérification automatique de tous les services
.\check-services.ps1
```

### 2. Configuration des variables d'environnement
```bash
$env:JAVA_HOME = "C:\Program Files\Java\jdk1.8.0_XXX"
$env:HADOOP_HOME = "C:\hadoop"
$env:SPARK_HOME = "C:\spark"
```

### 3. Activation de l'environnement virtuel
```bash
.\venv\Scripts\Activate.ps1
```

## Exécution

### Exécution individuelle
Chaque exercice peut être exécuté individuellement :

```bash
# Exercice 9 - Récupération historique
.\exo9\run_exercice9.ps1

# Exercice 10 - Records climatiques
.\exo10\run_exercice10.ps1

# Exercice 11 - Profils saisonniers
.\exo11\run_exercice11.ps1

# Exercice 12 - Validation & enrichissement
.\exo12\run_exercice12.ps1

# Exercice 13 - Détection d'anomalies
.\exo13\run_exercice13.ps1

# Exercice 14 - Interface web
.\exo14\run_exercice14.ps1
```

### Exécution globale
Pour exécuter tous les exercices dans l'ordre :

```bash
.\run_all_exercises_9_14.ps1
```

## Détails par exercice

### Exercice 9 - Récupération de séries historiques longues
- **Objectif** : Télécharger 10 ans de données météo via l'API archive Open-Meteo
- **Données** : Température, précipitations, vent, codes météo
- **Stockage** : Kafka (topic: `weather_history_raw`) + HDFS
- **Villes** : Paris, Lyon, Berlin, Madrid, Rome

### Exercice 10 - Détection des records climatiques locaux
- **Objectif** : Analyser les données historiques pour détecter les records
- **Records** : Jour le plus chaud/froid, rafale de vent max, jour le plus pluvieux
- **Technologie** : Spark job
- **Sortie** : Kafka (topic: `weather_records`) + HDFS

### Exercice 11 - Climatologie urbaine (profils saisonniers)
- **Objectif** : Créer des profils saisonniers par mois et par ville
- **Calculs** : Moyennes mensuelles, probabilités d'alerte
- **Technologie** : Spark job
- **Sortie** : HDFS (`/hdfs-data/{country}/{city}/seasonal_profile/`)

### Exercice 12 - Validation et enrichissement des profils saisonniers
- **Objectif** : Valider et enrichir les profils avec des statistiques avancées
- **Calculs** : Écart-type, min/max, médiane, quantiles, probabilités d'alerte
- **Technologie** : Spark job
- **Sortie** : HDFS (`/hdfs-data/{country}/{city}/seasonal_profile_enriched/`)

### Exercice 13 - Détection d'anomalies climatiques (Batch vs Speed)
- **Objectif** : Détecter les anomalies en temps réel
- **Source** : Kafka (topic: `weather_transformed`)
- **Référence** : Profils enrichis HDFS
- **Seuils** : Température (±5°C), vent (moyenne + 2 std), alertes (écart > 30%)
- **Technologie** : Spark Streaming
- **Sortie** : Kafka (topic: `weather_anomalies`) + HDFS

### Exercice 14 - Frontend global de visualisation météo
- **Objectif** : Interface web regroupant tous les dashboards
- **Technologies** : Flask, Bootstrap 5, Chart.js
- **Fonctionnalités** :
  - Dashboard temps réel
  - Visualisation historique
  - Détection d'anomalies
  - Comparaisons entre villes
- **URL** : http://localhost:5000

## Interfaces web

- **HDFS NameNode** : http://localhost:9870
- **YARN ResourceManager** : http://localhost:8088
- **Application Flask** : http://localhost:5000

## Structure des données

### Kafka Topics
- `weather_history_raw` : Données historiques brutes
- `weather_records` : Records climatiques détectés
- `weather_transformed` : Données temps réel transformées
- `weather_anomalies` : Anomalies détectées

### HDFS Structure
```
/hdfs-data/
├── {country}/
│   └── {city}/
│       ├── weather_history_raw/          # Exercice 9
│       ├── weather_records/              # Exercice 10
│       ├── seasonal_profile/             # Exercice 11
│       ├── seasonal_profile_enriched/    # Exercice 12
│       └── anomalies/                    # Exercice 13
```

## Dépannage

### Services non démarrés
```bash
# Redémarrage Hadoop
stop-dfs.cmd
stop-yarn.cmd
start-dfs.cmd
start-yarn.cmd

# Redémarrage Kafka
taskkill /F /IM java.exe
.\kafka\bin\windows\kafka-server-start.bat .\kafka\config\server.properties
```

### Vérification des données
```bash
# Topics Kafka
kafka-topics --bootstrap-server localhost:9092 --list

# Données HDFS
hdfs dfs -ls /hdfs-data/

# Logs détaillés
hdfs dfs -cat /hdfs-data/France/Paris/weather_history_raw/weather_history_2024-01-01.json
```

### Erreurs communes
1. **Services non démarrés** : Vérifier avec `.\check-services.ps1`
2. **Données manquantes** : Exécuter les exercices dans l'ordre
3. **Erreurs de permissions** : Vérifier les permissions HDFS
4. **Ports occupés** : Utiliser `netstat -ano | findstr :PORT`

## Support

Pour plus de détails sur chaque exercice, consultez les README individuels :
- `exo9/README.md`
- `exo10/README.md`
- `exo11/README.md`
- `exo12/README.md`
- `exo13/README.md`
- `exo14/README.md`
