# Exercice 6 : Extension du producteur avec géocodage

## Objectif
Modifier le producteur pour accepter ville et pays comme arguments, utiliser l'API de géocodage Open-Meteo pour obtenir les coordonnées, et inclure ces informations pour permettre le partitionnement par HDFS et les agrégats par région.

## Fichiers de rendu
- `enhanced_weather_producer.py` : Producteur étendu avec géocodage
- `README.md` : Ce fichier de documentation

## Utilisation

### Syntaxe
```bash
python enhanced_weather_producer.py <ville> [pays]
```

### Exemples
```bash
# Ville simple
python enhanced_weather_producer.py Paris
python enhanced_weather_producer.py Barcelona

# Ville avec pays
python enhanced_weather_producer.py Berlin Germany
python enhanced_weather_producer.py Tokyo Japan
```

## Fonctionnalités

### API de géocodage Open-Meteo
- URL : https://geocoding-api.open-meteo.com/v1/search
- Recherche par nom de ville avec ou sans pays
- Données enrichies : population, élévation, région, timezone

### Arguments flexibles
- Ville seule : géocodage automatique
- Ville + pays : géocodage précis
- Gestion d'erreurs et validation

### Données enrichies
- Localisation complète : ville, pays, région, département
- Métadonnées : population, élévation, timezone
- Codes ISO : code pays à 2 lettres
- Coordonnées précises géocodées

### Partitionnement HDFS
- Clé de partitionnement : PAYS/VILLE (ex: FR/Paris, JP/Tokyo)
- Structure hiérarchique pour stockage HDFS
- Compatible avec agrégats par pays/région

## Format des données
Les messages incluent maintenant :
- Informations géographiques complètes
- Métadonnées de localisation
- Structure prête pour partitionnement HDFS
- Support pour agrégats régionaux

## Prérequis
- Python 3.x avec environnement virtuel
- Dépendances : kafka-python, requests
- Connexion Internet pour APIs Open-Meteo
- Kafka démarré avec topic weather_stream