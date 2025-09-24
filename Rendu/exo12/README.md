# Exercice 12 - Validation et enrichissement des profils saisonniers

## Objectif

Job Spark qui valide la complétude et la plausibilité des données historiques, puis enrichit les profils saisonniers avec des statistiques détaillées et des probabilités d'alerte pour chaque mois et chaque ville.

## Fonctionnalités

- **Validation de complétude** : Vérification que chaque ville possède des données pour les 12 mois
- **Validation de plausibilité** : Vérification des valeurs météorologiques dans des plages réalistes
- **Calculs statistiques** : Écart-type, valeurs min/max, médiane, quantiles Q25/Q75
- **Probabilités d'alerte** : Calcul des probabilités d'alerte level_1 et level_2 par mois
- **Sauvegarde enrichie** : Profils complets avec toutes les statistiques dans HDFS

## Prérequis

- Python 3.8+
- Spark 3.5+
- HDFS en cours d'exécution
- Données historiques de l'exercice 9 disponibles dans HDFS

## Installation

```bash
pip install pyspark findspark
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
pip install pyspark findspark
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
- **HDFS NameNode** : Port 9870 (métadonnées HDFS)
- **HDFS DataNode** : Port 9864 (données HDFS)
- **YARN ResourceManager** : Port 8088 (gestion des ressources)
- **YARN NodeManager** : Port 8042 (exécution des tâches Spark)

### Exécution du script

**Commande principale :**
```bash
python profiles_validator_enricher.py
```

**Exécution avec PowerShell (Windows) :**
```powershell
# Dans le dossier exo12
python .\profiles_validator_enricher.py

# Ou via le script PowerShell
.\run_exercice12.ps1
```

**Exécution avec variables d'environnement Hadoop :**
```bash
# S'assurer que Hadoop est dans le PATH
export PATH=$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH
python profiles_validator_enricher.py
```

### Vérification des résultats

**1. Vérification HDFS :**
```bash
# Lister les profils enrichis créés
hdfs dfs -ls /hdfs-data/France/Paris/seasonal_profile_enriched/2024/

# Voir le contenu d'un profil enrichi
hdfs dfs -cat /hdfs-data/France/Paris/seasonal_profile_enriched/2024/profile.json | head -50
```

**2. Interface web HDFS :**
- Ouvrir : http://localhost:9870
- Naviguer vers : /hdfs-data/France/Paris/seasonal_profile_enriched/2024/

**3. Vérification de la validation :**
```bash
# Vérifier les résultats de validation pour toutes les villes
hdfs dfs -cat /hdfs-data/*/*/seasonal_profile_enriched/2024/profile.json | jq '.validation'
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

**Erreur de lecture HDFS :**
```bash
# Vérifier que les données de l'exercice 9 existent
hdfs dfs -ls -R /hdfs-data/

# Si les données n'existent pas, exécuter d'abord l'exercice 9
cd ../exo9
python weather_history_retriever.py
```

**Erreur "No data found" :**
```bash
# Vérifier le format des fichiers JSON dans HDFS
hdfs dfs -cat /hdfs-data/France/Paris/weather_history_raw/weather_history_2024.json | jq .city

# Si jq n'est pas installé, utiliser head pour voir le début du fichier
hdfs dfs -cat /hdfs-data/France/Paris/weather_history_raw/weather_history_2024.json | head -10
```

**Erreur de validation :**
```bash
# Vérifier la structure des données
hdfs dfs -cat /hdfs-data/France/Paris/weather_history_raw/weather_history_2024.json | jq '.daily | keys'

# Vérifier que les données contiennent les champs attendus
hdfs dfs -cat /hdfs-data/France/Paris/weather_history_raw/weather_history_2024.json | jq '.daily.time | length'
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

### Entrée HDFS

Le script lit les données brutes depuis :
```
/hdfs-data/{country}/{city}/weather_history_raw/*.json
```

### Sortie HDFS

Les profils enrichis sont sauvegardés dans :
```
/hdfs-data/{country}/{city}/seasonal_profile_enriched/{year}/profile.json
```

## Validation des données

### Complétude

- Vérification que chaque ville possède des données pour les 12 mois
- Détection des mois manquants
- Comptage des jours de données par mois

### Plausibilité

- **Température** : entre -50°C et +60°C
- **Vent** : entre 0 et 60 m/s
- Détection des valeurs aberrantes

## Calculs statistiques

Pour chaque mois et chaque ville :

### Température
- Moyenne (`temp_mean`)
- Écart-type (`temp_std`)
- Minimum (`temp_min`)
- Maximum (`temp_max`)
- Médiane (`temp_median`) - approximation
- Quantiles Q25 et Q75 (`temp_q25`, `temp_q75`) - approximation

### Vent
- Moyenne (`wind_mean`)
- Écart-type (`wind_std`)
- Minimum (`wind_min`)
- Maximum (`wind_max`)
- Médiane (`wind_median`) - approximation
- Quantiles Q25 et Q75 (`wind_q25`, `wind_q75`) - approximation

### Précipitations
- Moyenne (`precip_mean`)

## Probabilités d'alerte

Calcul basé sur les codes météo Open-Meteo :

### Level 1 (Alertes mineures)
- Codes : 61, 63, 65, 71, 73, 75, 77, 80, 81, 82, 45, 48
- Pluie légère, neige légère, brouillard

### Level 2 (Alertes majeures)
- Codes : 66, 67, 85, 86, 95, 96, 99
- Pluie forte, neige forte, orages, tempêtes

## Structure des données

### Format de sortie (HDFS)

```json
{
  "city": "Paris",
  "country": "France",
  "profile_year": 2024,
  "created_at": "2024-01-15T10:30:00",
  "monthly_profiles": [
    {
      "month": 1,
      "days_count": 31,
      "temp_mean": 4.1,
      "wind_mean": 12.3,
      "precip_mean": 45.2,
      "temp_std": 3.2,
      "wind_std": 2.1,
      "temp_min": -2.5,
      "temp_max": 12.8,
      "wind_min": 5.2,
      "wind_max": 28.7,
      "temp_median": 4.1,
      "wind_median": 12.3,
      "temp_q25": 1.8,
      "temp_q75": 6.4,
      "wind_q25": 10.2,
      "wind_q75": 14.4,
      "alert_probability_level_1": 0.15,
      "alert_probability_level_2": 0.05
    }
  ],
  "validation": {
    "completeness": true,
    "total_months": 12,
    "missing_months": []
  }
}
```

## Gestion des erreurs

Le script gère automatiquement :
- Les erreurs de lecture HDFS
- Les erreurs de traitement Spark
- Les données manquantes ou invalides
- Les interruptions utilisateur (Ctrl+C)
- La validation de la complétude et de la plausibilité

## Logs

Le script génère des logs détaillés incluant :
- Le nombre de fichiers traités depuis HDFS
- Le nombre d'enregistrements explosés
- Les résultats de validation de complétude
- Les résultats de validation de plausibilité
- Le traitement par ville et par mois
- Les erreurs de traitement
- Les confirmations de sauvegarde

## Performance

- Utilisation de Spark SQL pour l'analyse
- Regroupement optimisé par ville et mois
- Calculs statistiques avec gestion des valeurs nulles
- Sauvegarde par ville pour optimiser les écritures HDFS

## Exemple de sortie

```
=== EXERCICE 12: VALIDATION ET ENRICHISSEMENT DES PROFILS SAISONNIERS ===
Validation de complétude et plausibilité des données
Calcul de statistiques détaillées et probabilités d'alerte

2024-01-15 10:30:00 - INFO - Chargement des données historiques brutes depuis HDFS...
2024-01-15 10:30:01 - INFO - Données historiques chargées: 50 fichiers
2024-01-15 10:30:02 - INFO - Explosion des données quotidiennes...
2024-01-15 10:30:03 - INFO - Données explosées: 18250 enregistrements
2024-01-15 10:30:04 - INFO - Validation de la complétude des profils...
2024-01-15 10:30:05 - INFO - Validation de complétude terminée pour 5 villes
2024-01-15 10:30:06 - INFO - Validation de la plausibilité des données...
2024-01-15 10:30:07 - INFO - Validation de plausibilité terminée pour 5 villes
2024-01-15 10:30:08 - INFO - Enrichissement des profils avec statistiques...
2024-01-15 10:30:09 - INFO - Traitement des statistiques pour Paris, France
2024-01-15 10:30:10 - INFO - Profils enrichis créés pour 5 villes
2024-01-15 10:30:11 - INFO - Sauvegarde des profils enrichis dans HDFS...
2024-01-15 10:30:12 - INFO - Sauvegarde HDFS terminée

=== RESULTATS DE LA VALIDATION ET ENRICHISSEMENT ===

Validation de complétude:
  - Villes avec profils complets: 5/5

Validation de plausibilité:
  - Villes avec données plausibles: 5/5

Profils enrichis:
  - Nombre de villes traitées: 5
  - Paris, France: 12/12 mois
  - Lyon, France: 12/12 mois
  - Berlin, Germany: 12/12 mois
  - Madrid, Spain: 12/12 mois
  - Rome, Italy: 12/12 mois

Données sauvegardées dans HDFS:
  - Chemin: /hdfs-data/{country}/{city}/seasonal_profile_enriched/{year}/profile.json
```
