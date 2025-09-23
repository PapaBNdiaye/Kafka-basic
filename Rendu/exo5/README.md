# Exercice 5 : Agrégats en temps réel avec Spark

## Objectif
Calculer des agrégats en temps réel sur des fenêtres glissantes du flux weather_transformed avec Spark Streaming.

## Fonctionnalités
- Sliding window de 5 minutes sur weather_transformed
- Métriques calculées :
  - Nombre d'alertes level_1/level_2 par type (vent/chaleur)
  - Moyenne, min, max de la température
  - Nombre total d'alertes par ville

## Utilisation

### Prérequis
- Kafka et ZooKeeper démarrés
- Topic weather_transformed avec données (exercice 4)
- PySpark installé

### Commandes
```bash
# Lancement des agrégats Spark Streaming
python weather_aggregator.py
```

Le script lance un streaming qui traite les données en temps réel avec des fenêtres glissantes de 5 minutes.

## Métriques
- Agrégats par fenêtre de 5 minutes
- Groupement par coordonnées géographiques
- Calcul automatique des alertes significatives
- Affichage des résultats en temps réel