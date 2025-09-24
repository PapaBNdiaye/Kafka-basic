# Exercice 14 - Frontend global de visualisation météo

## Objectif

Interface web Flask qui regroupe tous les dashboards et visualisations issus des exercices précédents, offrant une vue d'ensemble complète des données météorologiques en temps réel, historiques, et des détections d'anomalies.

## Fonctionnalités

- **Dashboard temps réel** : Visualisation des données météo en streaming
- **Données historiques** : Analyse des données sur 10 ans avec graphiques interactifs
- **Détection d'anomalies** : Visualisation des anomalies détectées en temps réel
- **Comparaisons** : Comparaison des profils saisonniers entre villes
- **Interface responsive** : Design moderne avec Bootstrap 5
- **Graphiques interactifs** : Utilisation de Chart.js pour les visualisations

## Prérequis

- Python 3.8+
- Flask 2.0+
- Kafka en cours d'exécution
- HDFS en cours d'exécution
- Données disponibles dans Kafka et HDFS (exercices 9-13)

## Installation

```bash
pip install flask kafka-python requests
```

## Utilisation

### Prérequis

**IMPORTANT :** Les exercices précédents doivent être exécutés pour avoir des données :
- Exercice 9 : Données historiques dans HDFS
- Exercice 10 : Records climatiques dans Kafka
- Exercice 11 : Profils saisonniers dans HDFS
- Exercice 12 : Profils enrichis dans HDFS
- Exercice 13 : Anomalies dans Kafka (optionnel)

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
pip install flask kafka-python requests
```

3. **Vérification des services :**
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

# Vérifier que les topics Kafka existent
kafka-topics --bootstrap-server localhost:9092 --list
```

**Services requis complets :**
- **ZooKeeper** : Port 2181 (requis par Kafka)
- **Kafka** : Port 9092 (données temps réel)
- **HDFS NameNode** : Port 9870 (données historiques)
- **HDFS DataNode** : Port 9864 (données HDFS)
- **YARN ResourceManager** : Port 8088 (gestion des ressources)
- **YARN NodeManager** : Port 8042 (traitement des données)
- **Flask** : Port 5000 (interface web)

### Exécution de l'application

**Commande principale :**
```bash
python weather_dashboard_app.py
```

**Exécution avec PowerShell (Windows) :**
```powershell
# Dans le dossier exo14
python .\weather_dashboard_app.py

# Ou via le script PowerShell
.\run_exercice14.ps1
```

**L'application démarre sur :**
- URL : http://localhost:5000
- Port : 5000 (par défaut)
- Mode : Développement (avec rechargement automatique)

### Accès à l'interface web

**1. Page d'accueil :**
- URL : http://localhost:5000
- Vue d'ensemble du système
- Statut des services
- Navigation vers les modules

**2. Dashboard temps réel :**
- URL : http://localhost:5000/dashboard
- Données météo en streaming
- Graphiques interactifs
- Contrôles de mise à jour

**3. Données historiques :**
- URL : http://localhost:5000/historical
- Analyse sur 10 ans
- Sélection par ville
- Graphiques temporels

**4. Détection d'anomalies :**
- URL : http://localhost:5000/anomalies
- Visualisation des anomalies
- Filtres par type et ville
- Détails des détections

**5. Comparaisons :**
- URL : http://localhost:5000/comparisons
- Comparaison entre villes
- Profils saisonniers
- Statistiques comparatives

### Vérification du fonctionnement

**1. Vérification de l'application :**
```bash
# Vérifier que l'application répond
curl http://localhost:5000/api/status

# Ou ouvrir dans le navigateur
start http://localhost:5000
```

**2. Vérification des API :**
```bash
# Test des endpoints API
curl http://localhost:5000/api/cities
curl http://localhost:5000/api/realtime
curl http://localhost:5000/api/anomalies
```

**3. Vérification des logs :**
```bash
# Les logs s'affichent dans le terminal où l'application est lancée
# Surveiller les connexions Kafka et les erreurs éventuelles
```

### Configuration avancée

**Changement de port :**
```bash
# Modifier le port dans weather_dashboard_app.py
app.run(host='0.0.0.0', port=8080, debug=False)
```

**Configuration de production :**
```bash
# Utiliser un serveur WSGI comme Gunicorn
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 weather_dashboard_app:app
```

### Dépannage

**Erreur "Port already in use" :**
```bash
# Trouver le processus utilisant le port 5000
netstat -ano | findstr :5000

# Tuer le processus si nécessaire
taskkill /PID <PID> /F

# Ou changer le port dans le script
```

**Erreur de connexion Kafka :**
```bash
# Vérifier que Kafka est actif
kafka-topics --bootstrap-server localhost:9092 --list

# Vérifier les topics requis
kafka-topics --bootstrap-server localhost:9092 --list | grep -E "(weather_transformed|weather_anomalies|weather_records)"
```

**Erreur de connexion HDFS :**
```bash
# Vérifier que HDFS est actif
hdfs dfs -ls /

# Vérifier les données disponibles
hdfs dfs -ls -R /hdfs-data/
```

**Erreur "No data available" :**
```bash
# Vérifier que les exercices précédents ont été exécutés
hdfs dfs -ls /hdfs-data/France/Paris/

# Si les données manquent, exécuter les exercices requis
cd ../exo9
python weather_history_retriever.py
```

**Erreur de template :**
```bash
# Vérifier que les templates existent
ls -la templates/

# Vérifier les permissions de lecture
chmod 644 templates/*.html
```

**Performance lente :**
```bash
# Réduire la fréquence de mise à jour
# Modifier les intervalles dans les templates JavaScript
# Limiter le nombre de messages en cache
```

### Surveillance

**Logs de l'application :**
- Les logs s'affichent dans le terminal
- Niveau INFO par défaut
- Erreurs détaillées pour le débogage

**Métriques de performance :**
- Nombre de connexions Kafka actives
- Temps de réponse des API
- Utilisation mémoire

**Surveillance des données :**
- Vérifier que les consommateurs Kafka reçoivent des données
- Surveiller les erreurs de lecture HDFS
- Contrôler la qualité des données affichées

## Architecture

### Backend Flask
- **Application principale** : `weather_dashboard_app.py`
- **Consommateurs Kafka** : Threads en arrière-plan pour les données temps réel
- **API REST** : Endpoints pour fournir les données aux templates
- **Gestion des données** : Cache en mémoire pour les performances

### Frontend
- **Templates HTML** : Structure responsive avec Bootstrap 5
- **JavaScript** : Interactions dynamiques et graphiques
- **Chart.js** : Visualisations interactives
- **CSS** : Styles personnalisés pour l'interface

## Structure de l'application

```
Rendu/exo14/
├── weather_dashboard_app.py    # Application Flask principale
├── templates/
│   ├── base.html              # Template de base
│   ├── index.html             # Page d'accueil
│   ├── dashboard.html         # Dashboard temps réel
│   ├── historical.html        # Données historiques
│   ├── anomalies.html         # Détection d'anomalies
│   └── comparisons.html       # Comparaisons
└── README.md                  # Documentation
```

## Pages disponibles

### 1. Page d'accueil (`/`)
- Vue d'ensemble du système
- Statut des services (Kafka, HDFS, Spark)
- Statistiques globales
- Navigation vers les différents modules

### 2. Dashboard temps réel (`/dashboard`)
- Métriques en temps réel (température, vent, précipitations)
- Graphiques d'évolution temporelle
- Tableau des données récentes
- Contrôles de mise à jour et filtres

### 3. Données historiques (`/historical`)
- Sélection de ville par pays
- Graphiques d'évolution sur 10 ans
- Statistiques détaillées
- Analyse saisonnière
- Tableau des données mensuelles

### 4. Anomalies (`/anomalies`)
- Visualisation des anomalies détectées
- Filtres par type d'anomalie et ville
- Graphiques d'évolution des anomalies
- Détails des anomalies individuelles

### 5. Comparaisons (`/comparisons`)
- Comparaison entre deux villes
- Profils saisonniers superposés
- Statistiques comparatives
- Graphiques de comparaison

## API Endpoints

### Données temps réel
- `GET /api/realtime` : Dernières données temps réel
- `GET /api/anomalies` : Dernières anomalies détectées

### Données historiques
- `GET /api/historical/<country>/<city>` : Données historiques d'une ville
- `GET /api/seasonal/<country>/<city>` : Profil saisonnier d'une ville

### Métadonnées
- `GET /api/cities` : Liste des villes disponibles
- `GET /api/status` : Statut des services

## Configuration

### Topics Kafka
- `weather_transformed` : Données météo transformées (temps réel)
- `weather_anomalies` : Anomalies détectées
- `weather_records` : Records climatiques

### Données HDFS
- `/hdfs-data/{country}/{city}/weather_history_raw/` : Données historiques
- `/hdfs-data/{country}/{city}/seasonal_profile_enriched/` : Profils enrichis
- `/hdfs-data/{country}/{city}/anomalies/` : Anomalies sauvegardées

## Fonctionnalités techniques

### Consommateurs Kafka
- **Threads en arrière-plan** : Consommation continue des données
- **Cache en mémoire** : Stockage des dernières valeurs pour les performances
- **Gestion des erreurs** : Reconnexion automatique en cas de problème

### Visualisations
- **Graphiques temps réel** : Mise à jour automatique des données
- **Graphiques historiques** : Analyse sur plusieurs années
- **Graphiques comparatifs** : Superposition de données de plusieurs villes
- **Tableaux interactifs** : Filtrage et tri des données

### Interface utilisateur
- **Design responsive** : Adaptation à tous les écrans
- **Navigation intuitive** : Menu de navigation clair
- **Contrôles interactifs** : Filtres, sélecteurs, boutons de contrôle
- **Feedback visuel** : Indicateurs de statut et de chargement

## Gestion des erreurs

L'application gère automatiquement :
- Les erreurs de connexion Kafka
- Les erreurs de lecture HDFS
- Les données manquantes ou invalides
- Les erreurs de rendu des graphiques
- Les interruptions de service

## Performance

- **Cache en mémoire** : Limitation à 100 messages temps réel et 50 anomalies
- **Mise à jour configurable** : Intervalle de mise à jour ajustable
- **Optimisation des requêtes** : Requêtes API optimisées
- **Compression des données** : Réduction de la bande passante

## Exemple d'utilisation

```bash
# Démarrage de l'application
python weather_dashboard_app.py

# Accès à l'interface web
# http://localhost:5000

# Navigation dans les modules :
# - Accueil : Vue d'ensemble
# - Temps réel : Données en streaming
# - Historique : Analyse sur 10 ans
# - Anomalies : Détections en temps réel
# - Comparaisons : Profils saisonniers
```

## Surveillance

L'application peut être surveillée via :
- Les logs Flask en temps réel
- L'interface web pour les performances
- Les endpoints API pour les données
- Les métriques de consommation Kafka
- Les fichiers HDFS pour les sauvegardes

## Développement

Pour étendre l'application :
1. **Nouveaux endpoints** : Ajouter dans `weather_dashboard_app.py`
2. **Nouvelles pages** : Créer des templates HTML
3. **Nouvelles visualisations** : Étendre les graphiques Chart.js
4. **Nouveaux consommateurs** : Ajouter des threads Kafka
5. **Nouvelles données** : Intégrer de nouvelles sources
