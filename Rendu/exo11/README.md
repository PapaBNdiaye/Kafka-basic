# Exercice 11 - Climatologie urbaine (profils saisonniers)

## Objectif

Job Spark qui regroupe les données historiques par mois pour chaque ville afin de créer des profils saisonniers incluant la température moyenne, la vitesse du vent moyenne et les probabilités d'alerte météo.

## Fonctionnalités

- Analyse des données historiques stockées dans HDFS
- Regroupement des données par mois pour chaque ville
- Calcul des moyennes mensuelles :
  - Température moyenne par mois (profil saisonnier)
  - Vitesse du vent moyenne
  - Précipitations moyennes
- Calcul des probabilités d'alerte (level_1 et level_2) basées sur les codes météo
- Sauvegarde dans HDFS selon la structure `/hdfs-data/{country}/{city}/seasonal_profile`

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
python seasonal_profiles_analyzer.py
```

**Exécution avec PowerShell (Windows) :**
```powershell
# Dans le dossier exo11
python .\seasonal_profiles_analyzer.py

# Ou via le script PowerShell
.\run_exercice11.ps1
```

**Exécution avec variables d'environnement Hadoop :**
```bash
# S'assurer que Hadoop est dans le PATH
export PATH=$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH
python seasonal_profiles_analyzer.py
```

### Vérification des résultats

**1. Vérification HDFS :**
```bash
# Lister les profils saisonniers créés
hdfs dfs -ls /hdfs-data/France/Paris/seasonal_profile/

# Voir le contenu d'un profil saisonnier
hdfs dfs -cat /hdfs-data/France/Paris/seasonal_profile/seasonal_profile_2024.json | head -30
```

**2. Interface web HDFS :**
- Ouvrir : http://localhost:9870
- Naviguer vers : /hdfs-data/France/Paris/seasonal_profile/

**3. Vérification du contenu :**
```bash
# Vérifier la structure des profils pour toutes les villes
hdfs dfs -ls -R /hdfs-data/*/*/seasonal_profile/
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

Le script lit les données depuis :
```
/hdfs-data/{country}/{city}/weather_history_raw/*.json
```

### Sortie HDFS

Les profils saisonniers sont sauvegardés dans :
```
/hdfs-data/{country}/{city}/seasonal_profile/seasonal_profile_{year}.json
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

### Format de sortie (HDFS)

Les profils saisonniers au format JSON :
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
      "alert_probability_level_1": 0.15,
      "alert_probability_level_2": 0.05
    },
    {
      "month": 2,
      "days_count": 29,
      "temp_mean": 4.8,
      "wind_mean": 11.8,
      "precip_mean": 38.1,
      "alert_probability_level_1": 0.12,
      "alert_probability_level_2": 0.03
    }
  ],
  "validation": {
    "completeness": true,
    "total_months": 12,
    "missing_months": []
  }
}
```

## Calcul des probabilités d'alerte

Les probabilités d'alerte sont calculées basées sur les codes météo Open-Meteo :

### Level 1 (Alertes mineures)
- Pluie légère, neige légère, brouillard
- Codes : 61, 63, 65, 71, 73, 75, 77, 80, 81, 82, 45, 48

### Level 2 (Alertes majeures)
- Pluie forte, neige forte, orages, tempêtes
- Codes : 66, 67, 85, 86, 95, 96, 99

La probabilité est calculée comme : `nombre_de_jours_avec_alerte / nombre_total_de_jours`

## Gestion des erreurs

Le script gère automatiquement :
- Les erreurs de lecture HDFS
- Les erreurs de traitement Spark
- Les données manquantes ou invalides
- Les interruptions utilisateur (Ctrl+C)
- La validation de la complétude des profils (12 mois)

## Logs

Le script génère des logs détaillés incluant :
- Le nombre de fichiers traités depuis HDFS
- Le nombre d'enregistrements explosés
- Le traitement par ville et par mois
- Les erreurs de traitement
- Les confirmations de sauvegarde

## Performance

- Utilisation de Spark SQL pour l'analyse
- Regroupement optimisé par ville et mois
- Calcul des moyennes avec gestion des valeurs nulles
- Sauvegarde par ville pour optimiser les écritures HDFS

## Exemple de sortie

```
=== EXERCICE 11: CLIMATOLOGIE URBAINE (PROFILS SAISONNIERS) ===
Regroupement des données historiques par mois pour créer des profils saisonniers
Calcul des moyennes mensuelles et probabilités d'alerte

2024-01-15 10:30:00 - INFO - Chargement des données historiques depuis HDFS...
2024-01-15 10:30:01 - INFO - Données historiques chargées: 50 fichiers
2024-01-15 10:30:02 - INFO - Explosion des données quotidiennes...
2024-01-15 10:30:03 - INFO - Données explosées: 18250 enregistrements
2024-01-15 10:30:04 - INFO - Calcul des profils saisonniers...
2024-01-15 10:30:05 - INFO - Traitement des profils pour Paris, France
2024-01-15 10:30:06 - INFO - Traitement des profils pour Lyon, France
2024-01-15 10:30:07 - INFO - Traitement des profils pour Berlin, Germany
2024-01-15 10:30:08 - INFO - Traitement des profils pour Madrid, Spain
2024-01-15 10:30:09 - INFO - Traitement des profils pour Rome, Italy
2024-01-15 10:30:10 - INFO - Profils saisonniers créés pour 5 villes
2024-01-15 10:30:11 - INFO - Sauvegarde des profils dans HDFS...
2024-01-15 10:30:12 - INFO - Sauvegarde HDFS terminée

=== RESULTATS DES PROFILS SAISONNIERS ===

Profils créés pour 5 villes:

Paris, France: 12/12 mois
  - Janvier: Temp 4.1°C, Vent 12.3 km/h, Alertes L1: 15%, L2: 5%
  - Février: Temp 4.8°C, Vent 11.8 km/h, Alertes L1: 12%, L2: 3%
  - Mars: Temp 9.9°C, Vent 13.2 km/h, Alertes L1: 18%, L2: 7%
  - ...

Lyon, France: 12/12 mois
  - Janvier: Temp 3.2°C, Vent 11.5 km/h, Alertes L1: 14%, L2: 4%
  - Février: Temp 4.1°C, Vent 12.1 km/h, Alertes L1: 11%, L2: 2%
  - ...

Berlin, Germany: 12/12 mois
  - Janvier: Temp 1.8°C, Vent 13.8 km/h, Alertes L1: 16%, L2: 6%
  - Février: Temp 2.9°C, Vent 12.9 km/h, Alertes L1: 13%, L2: 3%
  - ...

Données sauvegardées dans HDFS:
  - Chemin: /hdfs-data/{country}/{city}/seasonal_profile/seasonal_profile_{year}.json
```
