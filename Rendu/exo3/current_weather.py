#!/usr/bin/env python3
"""
Exercice 3 : Producteur de données météo en direct
Script qui interroge l'API Open-Meteo pour une latitude/longitude
et envoie les données reçues dans le topic Kafka weather_stream
"""

import sys
import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer

class WeatherProducer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        """Initialise le producteur Kafka"""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None
        )
        self.api_base_url = "https://api.open-meteo.com/v1/forecast"
    
    def get_weather_data(self, latitude, longitude):
        """Récupère les données météo depuis l'API Open-Meteo"""
        try:
            # Paramètres pour l'API Open-Meteo
            params = {
                'latitude': latitude,
                'longitude': longitude,
                'current_weather': 'true',
                'timezone': 'auto'
            }
            
            print(f"Interrogation de l'API Open-Meteo pour lat={latitude}, lon={longitude}")
            response = requests.get(self.api_base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Extraction des données météo actuelles
            current_weather = data.get('current_weather', {})
            
            # Enrichissement des données
            weather_data = {
                'timestamp': datetime.now().isoformat(),
                'location': {
                    'latitude': latitude,
                    'longitude': longitude,
                    'timezone': data.get('timezone', 'UTC')
                },
                'current_weather': {
                    'time': current_weather.get('time'),
                    'temperature': current_weather.get('temperature'),
                    'windspeed': current_weather.get('windspeed'),
                    'winddirection': current_weather.get('winddirection'),
                    'weathercode': current_weather.get('weathercode'),
                    'is_day': current_weather.get('is_day')
                },
                'source': 'open-meteo-api',
                'exercice': 3
            }
            
            return weather_data
            
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de l'appel API: {e}")
            return None
        except Exception as e:
            print(f"Erreur lors du traitement des données: {e}")
            return None
    
    def send_weather_data(self, weather_data, topic='weather_stream'):
        """Envoie les données météo vers Kafka"""
        try:
            # Utilisation de la latitude comme clé pour le partitionnement
            key = f"{weather_data['location']['latitude']},{weather_data['location']['longitude']}"
            
            future = self.producer.send(topic, key=key, value=weather_data)
            
            # Attendre la confirmation d'envoi
            record_metadata = future.get(timeout=10)
            
            print(f"Donnees envoyees vers Kafka:")
            print(f"  Topic: {record_metadata.topic}")
            print(f"  Partition: {record_metadata.partition}")
            print(f"  Offset: {record_metadata.offset}")
            print(f"  Cle: {key}")
            print(f"  Temperature: {weather_data['current_weather']['temperature']}°C")
            print(f"  Vitesse du vent: {weather_data['current_weather']['windspeed']} m/s")
            
            return True
            
        except Exception as e:
            print(f"Erreur lors de l'envoi vers Kafka: {e}")
            return False
    
    def close(self):
        """Ferme le producteur"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            print("Producteur ferme proprement")

def main():
    """Fonction principale"""
    if len(sys.argv) != 3:
        print("Usage: python current_weather.py <latitude> <longitude>")
        print("Exemple: python current_weather.py 48.8566 2.3522  # Paris")
        print("Exemple: python current_weather.py 43.6043 1.4437   # Toulouse")
        sys.exit(1)
    
    try:
        latitude = float(sys.argv[1])
        longitude = float(sys.argv[2])
    except ValueError:
        print("Erreur: Latitude et longitude doivent etre des nombres")
        sys.exit(1)
    
    print(f"=== PRODUCTEUR METEO - EXERCICE 3 ===")
    print(f"Coordonnées: {latitude}, {longitude}")
    print(f"Topic Kafka: weather_stream")
    print("-" * 50)
    
    producer = None
    try:
        # Création du producteur
        producer = WeatherProducer()
        
        # Récupération des données météo
        weather_data = producer.get_weather_data(latitude, longitude)
        
        if weather_data:
            print("\n[OK] Données météo récupérées:")
            print(json.dumps(weather_data, indent=2, ensure_ascii=False))
            
            # Envoi vers Kafka
            print(f"\nEnvoi vers le topic weather_stream...")
            if producer.send_weather_data(weather_data):
                print("\n[SUCCESS] Streaming meteo reussi")
            else:
                print("\nEchec de l'envoi vers Kafka")
                sys.exit(1)
        else:
            print("Impossible de recuperer les donnees meteo")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nArret demande par l'utilisateur")
    except Exception as e:
        print(f"\nErreur: {e}")
        sys.exit(1)
    finally:
        if producer:
            producer.close()

if __name__ == "__main__":
    main()
