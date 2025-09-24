#!/usr/bin/env python3
"""
Exercice 14 : Frontend global de visualisation météo
- Créer une interface web qui regroupe tous les dashboards et visualisations
- Temps réel (données streaming)
- Historique (données stockées) 
- Comparaisons (profils saisonniers)
- Anomalies (détection en temps réel)
"""

from flask import Flask, render_template, jsonify, request
import json
import logging
from datetime import datetime, timedelta
import requests
from kafka import KafkaConsumer
import threading
import time
import os

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

class WeatherDashboardApp:
    def __init__(self):
        self.kafka_bootstrap_servers = ['localhost:9092']
        self.hdfs_base_url = "http://localhost:9870"
        self.realtime_data = []
        self.anomalies_data = []
        self.historical_data = {}
        self.seasonal_profiles = {}
        
        # Configuration des topics Kafka
        self.topics = {
            'weather_realtime': 'weather_transformed',
            'weather_anomalies': 'weather_anomalies',
            'weather_records': 'weather_records'
        }
        
        # Démarrer les consommateurs Kafka en arrière-plan
        self.start_kafka_consumers()

    def start_kafka_consumers(self):
        """Démarre les consommateurs Kafka pour les données temps réel"""
        try:
            # Consommateur pour les données temps réel
            realtime_thread = threading.Thread(
                target=self.consume_realtime_data,
                daemon=True
            )
            realtime_thread.start()
            
            # Consommateur pour les anomalies
            anomalies_thread = threading.Thread(
                target=self.consume_anomalies_data,
                daemon=True
            )
            anomalies_thread.start()
            
            logger.info("Consommateurs Kafka démarrés")
            
        except Exception as e:
            logger.error(f"Erreur lors du démarrage des consommateurs Kafka: {e}")

    def consume_realtime_data(self):
        """Consomme les données météo temps réel depuis Kafka"""
        try:
            consumer = KafkaConsumer(
                self.topics['weather_realtime'],
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest'
            )
            
            logger.info("Consommateur données temps réel démarré")
            
            for message in consumer:
                data = message.value
                data['kafka_timestamp'] = datetime.now().isoformat()
                
                # Garde seulement les 100 dernières valeurs
                self.realtime_data.append(data)
                if len(self.realtime_data) > 100:
                    self.realtime_data.pop(0)
                
                logger.debug(f"Donnée temps réel reçue: {data.get('city', 'Unknown')}")
                
        except Exception as e:
            logger.error(f"Erreur dans le consommateur temps réel: {e}")

    def consume_anomalies_data(self):
        """Consomme les données d'anomalies depuis Kafka"""
        try:
            consumer = KafkaConsumer(
                self.topics['weather_anomalies'],
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest'
            )
            
            logger.info("Consommateur anomalies démarré")
            
            for message in consumer:
                data = message.value
                data['kafka_timestamp'] = datetime.now().isoformat()
                
                # Garde seulement les 50 dernières anomalies
                self.anomalies_data.append(data)
                if len(self.anomalies_data) > 50:
                    self.anomalies_data.pop(0)
                
                logger.info(f"Anomalie détectée: {data.get('city', 'Unknown')} - {data.get('anomaly_types', [])}")
                
        except Exception as e:
            logger.error(f"Erreur dans le consommateur anomalies: {e}")

    def load_historical_data_from_hdfs(self, city, country):
        """Charge les données historiques depuis HDFS"""
        try:
            # Simulation de chargement depuis HDFS
            # En production, utiliser l'API HDFS ou hdfs3
            
            historical_data = {
                "city": city,
                "country": country,
                "years": [2020, 2021, 2022, 2023, 2024],
                "monthly_stats": {
                    "temperature": {
                        "jan": [2.1, 1.8, 3.2, 4.1, 3.9],
                        "feb": [3.2, 2.9, 4.1, 5.2, 4.8],
                        "mar": [7.8, 8.1, 9.2, 10.1, 9.9],
                        "apr": [12.3, 13.1, 14.2, 15.1, 14.8],
                        "may": [17.2, 18.1, 19.2, 20.1, 19.9],
                        "jun": [21.1, 22.1, 23.2, 24.1, 23.9],
                        "jul": [24.2, 25.1, 26.2, 27.1, 26.9],
                        "aug": [23.8, 24.1, 25.2, 26.1, 25.9],
                        "sep": [19.2, 20.1, 21.2, 22.1, 21.9],
                        "oct": [13.1, 14.1, 15.2, 16.1, 15.9],
                        "nov": [7.8, 8.1, 9.2, 10.1, 9.9],
                        "dec": [4.2, 3.9, 5.1, 6.1, 5.9]
                    },
                    "precipitation": {
                        "jan": [45, 52, 38, 41, 43],
                        "feb": [38, 41, 35, 39, 37],
                        "mar": [42, 48, 39, 44, 41],
                        "apr": [38, 45, 32, 39, 36],
                        "may": [52, 58, 48, 55, 51],
                        "jun": [48, 52, 44, 49, 47],
                        "jul": [58, 62, 52, 59, 56],
                        "aug": [52, 58, 48, 55, 51],
                        "sep": [45, 48, 42, 46, 44],
                        "oct": [52, 58, 48, 55, 51],
                        "nov": [48, 52, 44, 49, 47],
                        "dec": [45, 48, 42, 46, 44]
                    }
                }
            }
            
            return historical_data
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement des données historiques: {e}")
            return None

    def load_seasonal_profiles(self, city, country):
        """Charge les profils saisonniers depuis HDFS"""
        try:
            # Simulation de chargement depuis HDFS
            seasonal_profile = {
                "city": city,
                "country": country,
                "monthly_profiles": [
                    {"month": 1, "temp_mean": 4.1, "wind_mean": 12.3, "precip_mean": 45.2},
                    {"month": 2, "temp_mean": 4.8, "wind_mean": 11.8, "precip_mean": 38.1},
                    {"month": 3, "temp_mean": 9.9, "wind_mean": 13.2, "precip_mean": 42.8},
                    {"month": 4, "temp_mean": 14.8, "wind_mean": 12.8, "precip_mean": 38.0},
                    {"month": 5, "temp_mean": 19.9, "wind_mean": 11.5, "precip_mean": 52.3},
                    {"month": 6, "temp_mean": 23.9, "wind_mean": 10.8, "precip_mean": 48.2},
                    {"month": 7, "temp_mean": 26.9, "wind_mean": 10.2, "precip_mean": 58.1},
                    {"month": 8, "temp_mean": 25.9, "wind_mean": 10.5, "precip_mean": 52.8},
                    {"month": 9, "temp_mean": 21.9, "wind_mean": 11.8, "precip_mean": 45.2},
                    {"month": 10, "temp_mean": 15.9, "wind_mean": 12.8, "precip_mean": 52.8},
                    {"month": 11, "temp_mean": 9.9, "wind_mean": 13.2, "precip_mean": 48.2},
                    {"month": 12, "temp_mean": 5.9, "wind_mean": 12.8, "precip_mean": 45.2}
                ]
            }
            
            return seasonal_profile
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement des profils saisonniers: {e}")
            return None

# Instance globale de l'application
dashboard_app = WeatherDashboardApp()

# Routes Flask
@app.route('/')
def index():
    """Page d'accueil du dashboard"""
    return render_template('index.html')

@app.route('/dashboard')
def dashboard():
    """Dashboard temps réel"""
    return render_template('dashboard.html')

@app.route('/historical')
def historical():
    """Visualisation des données historiques"""
    return render_template('historical.html')

@app.route('/anomalies')
def anomalies():
    """Dashboard des anomalies"""
    return render_template('anomalies.html')

@app.route('/comparisons')
def comparisons():
    """Comparaisons et profils saisonniers"""
    return render_template('comparisons.html')

# API Endpoints
@app.route('/api/realtime')
def api_realtime():
    """API pour les données temps réel"""
    return jsonify({
        'data': dashboard_app.realtime_data[-20:],  # 20 dernières valeurs
        'count': len(dashboard_app.realtime_data),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/anomalies')
def api_anomalies():
    """API pour les anomalies"""
    return jsonify({
        'data': dashboard_app.anomalies_data[-10:],  # 10 dernières anomalies
        'count': len(dashboard_app.anomalies_data),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/historical/<country>/<city>')
def api_historical(country, city):
    """API pour les données historiques d'une ville"""
    historical_data = dashboard_app.load_historical_data_from_hdfs(city, country)
    return jsonify(historical_data)

@app.route('/api/seasonal/<country>/<city>')
def api_seasonal(country, city):
    """API pour les profils saisonniers d'une ville"""
    seasonal_profile = dashboard_app.load_seasonal_profiles(city, country)
    return jsonify(seasonal_profile)

@app.route('/api/cities')
def api_cities():
    """API pour la liste des villes disponibles"""
    cities = [
        {"city": "Paris", "country": "France"},
        {"city": "Lyon", "country": "France"},
        {"city": "Berlin", "country": "Germany"},
        {"city": "Madrid", "country": "Spain"},
        {"city": "Rome", "country": "Italy"}
    ]
    return jsonify(cities)

@app.route('/api/status')
def api_status():
    """API pour le statut des services"""
    status = {
        'kafka': {
            'connected': len(dashboard_app.realtime_data) > 0,
            'topics': list(dashboard_app.topics.values()),
            'last_update': datetime.now().isoformat()
        },
        'hdfs': {
            'accessible': True,  # Simulation
            'base_url': dashboard_app.hdfs_base_url
        },
        'data': {
            'realtime_count': len(dashboard_app.realtime_data),
            'anomalies_count': len(dashboard_app.anomalies_data)
        }
    }
    return jsonify(status)

# Templates Flask (simplifiés)
@app.route('/templates/base.html')
def base_template():
    """Template de base"""
    return """
    <!DOCTYPE html>
    <html lang="fr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Weather Dashboard</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    </head>
    <body>
        <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
            <div class="container">
                <a class="navbar-brand" href="/">Weather Dashboard</a>
                <div class="navbar-nav">
                    <a class="nav-link" href="/dashboard">Temps réel</a>
                    <a class="nav-link" href="/historical">Historique</a>
                    <a class="nav-link" href="/anomalies">Anomalies</a>
                    <a class="nav-link" href="/comparisons">Comparaisons</a>
                </div>
            </div>
        </nav>
        <div class="container mt-4">
            {% block content %}{% endblock %}
        </div>
    </body>
    </html>
    """

@app.route('/templates/index.html')
def index_template():
    """Template de la page d'accueil"""
    return """
    {% extends "base.html" %}
    {% block content %}
    <div class="row">
        <div class="col-12">
            <h1>Weather Dashboard - Tableau de bord météo</h1>
            <p class="lead">Interface de visualisation des données météorologiques en temps réel et historiques</p>
        </div>
    </div>
    <div class="row mt-4">
        <div class="col-md-3">
            <div class="card text-white bg-primary">
                <div class="card-body">
                    <h5 class="card-title">Temps réel</h5>
                    <p class="card-text">Données météo en streaming</p>
                    <a href="/dashboard" class="btn btn-light">Accéder</a>
                </div>
            </div>
        </div>
        <div class="col-md-3">
            <div class="card text-white bg-info">
                <div class="card-body">
                    <h5 class="card-title">Historique</h5>
                    <p class="card-text">Données sur 10 ans</p>
                    <a href="/historical" class="btn btn-light">Accéder</a>
                </div>
            </div>
        </div>
        <div class="col-md-3">
            <div class="card text-white bg-warning">
                <div class="card-body">
                    <h5 class="card-title">Anomalies</h5>
                    <p class="card-text">Détection en temps réel</p>
                    <a href="/anomalies" class="btn btn-light">Accéder</a>
                </div>
            </div>
        </div>
        <div class="col-md-3">
            <div class="card text-white bg-success">
                <div class="card-body">
                    <h5 class="card-title">Comparaisons</h5>
                    <p class="card-text">Profils saisonniers</p>
                    <a href="/comparisons" class="btn btn-light">Accéder</a>
                </div>
            </div>
        </div>
    </div>
    {% endblock %}
    """

def main():
    """Fonction principale"""
    print("=== EXERCICE 14: FRONTEND GLOBAL DE VISUALISATION METEO ===")
    print("Interface web Flask pour visualiser toutes les données météo")
    print("Intégration temps réel, historique, anomalies et comparaisons")
    print()
    
    logger.info("Démarrage du serveur Flask...")
    
    try:
        # Démarrage du serveur Flask
        app.run(
            host='0.0.0.0',
            port=5000,
            debug=False,
            threaded=True
        )
        
    except Exception as e:
        logger.error(f"Erreur lors du démarrage du serveur: {e}")

if __name__ == "__main__":
    main()
