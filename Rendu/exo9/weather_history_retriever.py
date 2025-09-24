#!/usr/bin/env python3
"""
Exercice 9 : Récupération de séries historiques longues
- Télécharge des données météo sur 10 ans pour une ville donnée via l'API archive
- Stocke les données brutes dans Kafka : weather_history_raw
- Sauvegarde les données dans HDFS : /hdfs-data/{country}/{city}/weather_history_raw
"""

import requests
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
from hdfs import InsecureClient
import os

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherHistoryRetriever:
    def __init__(self):
        # Configuration Kafka
        self.kafka_bootstrap_servers = ['localhost:9092']
        self.kafka_topic = 'weather_history_raw'
        
        # Configuration HDFS
        self.hdfs_client = InsecureClient('http://localhost:9870', user='dell')
        
        # Configuration API
        self.archive_api_base = 'https://archive-api.open-meteo.com/v1/archive'
        
        # Paramètres par défaut pour les villes
        self.cities = {
            'Paris': {'latitude': 48.8566, 'longitude': 2.3522, 'country': 'France'},
            'Lyon': {'latitude': 45.7640, 'longitude': 4.8357, 'country': 'France'},
            'Berlin': {'latitude': 52.5200, 'longitude': 13.4050, 'country': 'Germany'},
            'Madrid': {'latitude': 40.4168, 'longitude': -3.7038, 'country': 'Spain'},
            'Rome': {'latitude': 41.9028, 'longitude': 12.4964, 'country': 'Italy'}
        }
        
        # Variables météo à récupérer
        self.weather_variables = [
            'temperature_2m_max',
            'temperature_2m_min', 
            'precipitation_sum',
            'windspeed_10m_max',
            'weathercode'
        ]
        
        # Initialisation du producteur Kafka
        self.producer = None
        self.init_kafka_producer()

    def init_kafka_producer(self):
        """Initialise le producteur Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3,
                retry_backoff_ms=1000
            )
            logger.info("Producteur Kafka initialisé")
        except Exception as e:
            logger.error(f"Erreur lors de l'initialisation du producteur Kafka: {e}")

    def retrieve_10_years_data(self, city_name, test_mode=False):
        """
        Récupère les données météo sur 10 ans pour une ville donnée
        
        Args:
            city_name (str): Nom de la ville
            test_mode (bool): Si True, récupère seulement 1 an de données
        """
        if city_name not in self.cities:
            logger.error(f"Ville non supportée: {city_name}")
            return False
        
        city_info = self.cities[city_name]
        logger.info(f"Début de la récupération des données pour {city_name}")
        
        # Déterminer le nombre d'années à récupérer
        years_back = 1 if test_mode else 10
        current_date = datetime.now()
        
        total_days = 0
        successful_requests = 0
        
        for year_offset in range(years_back):
            target_year = current_date.year - year_offset
            start_date = f"{target_year}-01-01"
            end_date = f"{target_year}-12-31"
            
            logger.info(f"Récupération des données pour {target_year}")
            
            # Construction de l'URL API
            url = f"{self.archive_api_base}?"
            url += f"latitude={city_info['latitude']}&"
            url += f"longitude={city_info['longitude']}&"
            url += f"start_date={start_date}&"
            url += f"end_date={end_date}&"
            url += f"daily={','.join(self.weather_variables)}"
            
            try:
                # Appel API avec timeout
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # Ajout des métadonnées
                data['city'] = city_name
                data['country'] = city_info['country']
                data['retrieval_date'] = datetime.now().isoformat()
                data['period_start'] = start_date
                data['period_end'] = end_date
                data['year'] = target_year
                
                # Compter les jours de données
                if 'daily' in data and 'time' in data['daily']:
                    days_count = len(data['daily']['time'])
                    total_days += days_count
                    logger.info(f"Données récupérées pour {target_year}: {days_count} jours")
                    
                    # Envoi vers Kafka
                    self.send_to_kafka(data)
                    
                    # Sauvegarde dans HDFS
                    self.save_to_hdfs(data, city_info['country'], city_name, target_year)
                    
                    successful_requests += 1
                else:
                    logger.warning(f"Aucune donnée quotidienne trouvée pour {target_year}")
                
                # Pause entre les requêtes pour éviter la surcharge
                time.sleep(1)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur lors de la requête API pour {target_year}: {e}")
            except Exception as e:
                logger.error(f"Erreur lors du traitement des données pour {target_year}: {e}")
        
        logger.info(f"Récupération terminée: {successful_requests}/{years_back} années, {total_days} jours total")
        return successful_requests > 0

    def send_to_kafka(self, data):
        """Envoie les données vers Kafka"""
        try:
            if self.producer:
                future = self.producer.send(self.kafka_topic, data)
                future.get(timeout=10)
                logger.debug(f"Données envoyées vers Kafka topic: {self.kafka_topic}")
            else:
                logger.error("Producteur Kafka non initialisé")
        except KafkaError as e:
            logger.error(f"Erreur lors de l'envoi vers Kafka: {e}")
        except Exception as e:
            logger.error(f"Erreur inattendue lors de l'envoi vers Kafka: {e}")

    def save_to_hdfs(self, data, country, city, year):
        """Sauvegarde les données dans HDFS"""
        try:
            # Construction du chemin HDFS
            hdfs_path = f"/hdfs-data/{country}/{city}/weather_history_raw"
            
            # Création du répertoire s'il n'existe pas
            try:
                self.hdfs_client.makedirs(hdfs_path)
            except:
                pass  # Le répertoire existe peut-être déjà
            
            # Nom du fichier
            filename = f"weather_history_{year}.json"
            file_path = f"{hdfs_path}/{filename}"
            
            # Sauvegarde du fichier
            json_data = json.dumps(data, indent=2)
            self.hdfs_client.write(file_path, json_data, overwrite=True)
            
            logger.info(f"Données sauvegardées dans HDFS: {file_path}")
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde HDFS: {e}")

    def get_city_list(self):
        """Retourne la liste des villes disponibles"""
        return list(self.cities.keys())

    def close(self):
        """Ferme les connexions"""
        if self.producer:
            self.producer.close()
            logger.info("Producteur Kafka fermé")

def main():
    """Fonction principale"""
    print("=== EXERCICE 9: RECUPERATION DE SERIES HISTORIQUES LONGUES ===")
    print("Téléchargement de données météo sur 10 ans via l'API archive Open-Meteo")
    print("Stockage dans Kafka et HDFS")
    print()
    
    retriever = WeatherHistoryRetriever()
    
    try:
        # Mode test automatique pour Paris avec 1 an de données
        test_mode = False  # Changer à True pour tester avec 1 an seulement
        
        print(f"Mode test: {'Activé (1 an)' if test_mode else 'Désactivé (10 ans)'}")
        print(f"Ville sélectionnée: Paris")
        print()
        
        # Récupération des données
        success = retriever.retrieve_10_years_data('Paris', test_mode=test_mode)
        
        if success:
            print("Récupération des données terminée avec succès")
            print("Données disponibles dans:")
            print("- Kafka topic: weather_history_raw")
            print("- HDFS: /hdfs-data/France/Paris/weather_history_raw/")
        else:
            print("Erreur lors de la récupération des données")
            
    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur")
    except Exception as e:
        print(f"Erreur inattendue: {e}")
    finally:
        retriever.close()

if __name__ == "__main__":
    main()
