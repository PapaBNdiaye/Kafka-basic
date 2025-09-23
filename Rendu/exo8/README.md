# Exercice 8 : Visualisation et agrégation des logs météo

## Objectif
Consommer les logs HDFS et implémenter les visualisations : évolution température/vent, alertes par niveau, codes météo par pays.

## Fichiers de rendu
- `weather_visualizer.py` : Visualisateur avec pandas et matplotlib
- `README.md` : Ce fichier de documentation

## Utilisation

### Génération des visualisations
```bash
python weather_visualizer.py
```

## Visualisations implémentées

### 1. Évolution de la température au fil du temps
- Fichier : `temperature_evolution.png`
- Contenu : Courbes par ville (top 5)
- Données : Observations avec température de 12.9°C à 28.5°C

### 2. Évolution de la vitesse du vent
- Fichier : `wind_evolution.png`
- Contenu : Courbes par ville (top 5)
- Données : Vitesse de 1.8 à 31.0 m/s

### 3. Nombre d'alertes vent et chaleur par niveau
- Fichier : `alerts_by_level.png`
- Contenu : Graphiques en barres
- Vent : level_0, level_1, level_2
- Chaleur : level_0, level_1, level_2

### 4. Code météo le plus fréquent par pays
- Fichier : `weather_codes_by_country.png`
- Contenu : Graphiques en secteurs par pays
- Pays analysés : 6 pays (FR, ES, IT, BE, DE, UNKNOWN)

## Données analysées

### Source : Structure HDFS (exercice 7)
```
hdfs-data/
├── BE/Bruxelles/
├── DE/Berlin/
├── ES/Madrid/
├── FR/
│   ├── Lyon/
│   ├── Paris/
│   └── Toulouse/
├── IT/Rome/
└── UNKNOWN/Unknown_Location/
```

### Statistiques globales
- Observations analysées
- Pays représentés
- Villes différentes
- Période : Données temps réel du TP

### Métriques météo
- Température : Moyenne et plage
- Vent : Moyenne et plage
- Alertes significatives : level_1 et level_2

## Types de graphiques
- Courbes temporelles pour évolution température/vent
- Graphiques en barres pour alertes par niveau
- Graphiques en secteurs pour codes météo par pays

## Pipeline
HDFS (ex7) → Chargement données → Analyse → Visualisations (ex8)

## Prérequis
- Python 3.x avec environnement virtuel
- Dépendances : matplotlib, pandas, seaborn, numpy
- Données HDFS de l'exercice 7
- Structure hdfs-data/ accessible