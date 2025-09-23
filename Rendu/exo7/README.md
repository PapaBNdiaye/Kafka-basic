# Exercice 7 : Stockage dans HDFS organisé

## Objectif
Écrire un consommateur Kafka qui lit weather_transformed et sauvegarde les alertes dans HDFS selon la structure : /hdfs-data/{country}/{city}/alerts.json

## Fichiers de rendu
- `hdfs_storage.py` : Consommateur avec stockage HDFS
- `README.md` : Ce fichier de documentation

## Utilisation

### Test du stockage HDFS
```bash
python hdfs_storage.py
```

## Fonctionnalités

### Détection automatique HDFS
- HDFS réel : Utilise hdfs3 si HDFS disponible
- Simulation locale : Structure identique en local si HDFS absent
- Compatibilité : Fonctionne dans tous les environnements

### Structure HDFS organisée
```
hdfs-data/
├── FR/                    # France
│   ├── Paris/
│   │   └── alerts.json
│   ├── Toulouse/
│   │   └── alerts.json
│   └── Lyon/
│       └── alerts.json
├── ES/                    # Espagne
│   ├── Madrid/
│   │   └── alerts.json
│   └── Seville/
│       └── alerts.json
├── IT/                    # Italie
│   └── Rome/
│       └── alerts.json
└── [autres pays...]
```

### Extraction de localisation
- Messages enrichis : Utilise country_code/city (exercice 6)
- Messages coordonnées : Mapping automatique vers pays/ville
- Gestion d'erreurs : Dossier UNKNOWN pour données non reconnues

### Format des alertes sauvegardées
Les alertes sont sauvegardées en JSON avec :
- Timestamp et métadonnées de localisation
- Données météo et niveaux d'alertes
- Métadonnées Kafka (offset, partition, source)

## Résultats attendus
- Messages traités : Alertes depuis weather_transformed
- Fichiers créés : Structure organisée par pays/ville
- Pays organisés : FR, ES, IT, JP, GB, etc.
- Structure hiérarchique : Parfaitement organisée

## Pipeline
weather_transformed → Consommateur HDFS → Structure organisée par pays/ville

## Avantages
1. Partitionnement géographique : Données organisées par pays puis ville
2. Scalabilité : Structure adaptée aux gros volumes de données
3. Requêtes optimisées : Accès direct par pays/ville
4. Compatibilité : Fonctionne avec données des exercices 4-6
5. Flexibilité : HDFS réel ou simulation selon environnement

## Données d'exemple
Le dossier `hdfs-data/` contient des données d'exemple générées par le système :
- Structure organisée par pays/ville
- Fichiers `alerts.json` avec données météo et alertes
- 8 villes dans 6 pays (FR, ES, IT, DE, BE, UNKNOWN)
- Données prêtes pour l'exercice 8 (visualisation)

## Prérequis
- Python 3.x avec environnement virtuel
- Dépendances : kafka-python, hdfs3
- Topic weather_transformed avec données (exercices 4-6)
- HDFS optionnel (simulation automatique si absent)
- Kafka démarré