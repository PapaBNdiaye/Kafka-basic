# Exercice 3 : Producteur de données météo

## Objectif
Créer un script qui récupère les données météo via l'API Open-Meteo et les envoie dans Kafka.

## Utilisation

### Prérequis
- Kafka et ZooKeeper démarrés
- Topic weather_stream créé
- Python avec kafka-python et requests installés

### Commandes
```bash
# Activation de l'environnement
cd TP\solution\exo3
venv\Scripts\Activate.ps1

# Exécution du script
python current_weather.py <latitude> <longitude>

# Exemples
python current_weather.py 48.8566 2.3522  # Paris
python current_weather.py 43.6043 1.4437  # Toulouse
```

### Résultat
Le script interroge l'API Open-Meteo et envoie les données météo vers le topic weather_stream avec les informations suivantes :
- Coordonnées GPS
- Température actuelle
- Vitesse et direction du vent
- Code météo
- Timestamp

### Gestion des erreurs
- Vérification des arguments (latitude/longitude valides)
- Gestion des erreurs réseau
- Vérification de la connexion Kafka
- Messages d'erreur clairs