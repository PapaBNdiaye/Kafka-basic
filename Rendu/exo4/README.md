# Exercice 4 : Transformation des données et détection d'alertes

## Objectif
Traiter le flux weather_stream avec Spark et produire weather_transformed avec alertes de vent et chaleur.

## Fonctionnalités
- Lecture du topic weather_stream
- Calcul des alertes selon les seuils :
  - Vent faible (< 10 m/s) → level_0
  - Vent modéré (10-20 m/s) → level_1  
  - Vent fort (> 20 m/s) → level_2
  - Température normale (< 25°C) → level_0
  - Chaleur modérée (25-35°C) → level_1
  - Canicule (> 35°C) → level_2
- Envoi vers topic weather_transformed

## Utilisation

### Prérequis
- Kafka et ZooKeeper démarrés
- Topic weather_stream avec données (exercice 3)
- Topic weather_transformed créé

### Commandes
```bash
# Création du topic weather_transformed
powershell -ExecutionPolicy Bypass -File setup_topics.ps1

# Lancement du Spark Streaming
python weather_transformer.py
```

### Colonnes ajoutées
- event_time : timestamp de traitement
- wind_alert_level : niveau alerte vent
- heat_alert_level : niveau alerte chaleur
- temperature et windspeed transformés