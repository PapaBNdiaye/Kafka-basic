#!/usr/bin/env python3
"""
Exercice 6 : Extension du producteur avec geocodage
Producteur qui accepte ville et pays comme arguments et utilise l'API de geocodage Open-Meteo
pour obtenir les coordonnees puis les donnees meteo
"""

import sys
import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer

class EnhancedWeatherProducer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        """Initialise le producteur Kafka etendu"""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None
        )
        self.geocoding_url = "https://geocoding-api.open-meteo.com/v1/search"
        self.weather_url = "https://api.open-meteo.com/v1/forecast"
    
    def geocode_location(self, city, country=None):
        """Obtient les coordonnees d'une ville via l'API de geocodage Open-Meteo"""
        try:
            # Preparation de la requete de geocodage
            search_query = city
            if country:
                search_query = f"{city}, {country}"
            
            params = {
                'name': search_query,
                'count': 1,
                'language': 'fr',
                'format': 'json'
            }
            
            print(f"Geocodage de '{search_query}'...")
            response = requests.get(self.geocoding_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if not data.get('results'):
                print(f"Aucun resultat trouve pour '{search_query}'")
                return None
            
            location = data['results'][0]
            
            geocoding_result = {
                'latitude': location.get('latitude'),
                'longitude': location.get('longitude'),
                'name': location.get('name'),
                'country': location.get('country'),
                'country_code': location.get('country_code'),
                'admin1': location.get('admin1'),  # Region/Etat
                'admin2': location.get('admin2'),  # Departement/Province
                'timezone': location.get('timezone'),
                'population': location.get('population'),
                'elevation': location.get('elevation')
            }
            
            print(f"Localisation trouvee:")
            print(f"  Nom: {geocoding_result['name']}")
            print(f"  Pays: {geocoding_result['country']} ({geocoding_result['country_code']})")
            print(f"  Coordonnees: {geocoding_result['latitude']}, {geocoding_result['longitude']}")
            print(f"  Timezone: {geocoding_result['timezone']}")
            
            return geocoding_result
            
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors du geocodage: {e}")
            return None
        except Exception as e:
            print(f"Erreur lors du traitement du geocodage: {e}")
            return None
    
    def get_weather_data(self, location_info):
        """Recupere les donnees meteo pour une localisation geocodee"""
        try:
            latitude = location_info['latitude']
            longitude = location_info['longitude']
            
            # Parametres pour l'API meteo
            params = {
                'latitude': latitude,
                'longitude': longitude,
                'current_weather': 'true',
                'timezone': location_info.get('timezone', 'auto')
            }
            
            print(f"Recuperation donnees meteo...")
            response = requests.get(self.weather_url, params=params, timeout=10)
            response.raise_for_status()
            
            weather_data = response.json()
            current_weather = weather_data.get('current_weather', {})
            
            # Donnees meteo enrichies avec informations de localisation
            enhanced_weather = {
                'timestamp': datetime.now().isoformat(),
                'location': {
                    'city': location_info['name'],
                    'country': location_info['country'],
                    'country_code': location_info['country_code'],
                    'region': location_info.get('admin1'),
                    'department': location_info.get('admin2'),
                    'latitude': latitude,
                    'longitude': longitude,
                    'timezone': location_info.get('timezone'),
                    'population': location_info.get('population'),
                    'elevation': location_info.get('elevation')
                },
                'current_weather': {
                    'time': current_weather.get('time'),
                    'temperature': current_weather.get('temperature'),
                    'windspeed': current_weather.get('windspeed'),
                    'winddirection': current_weather.get('winddirection'),
                    'weathercode': current_weather.get('weathercode'),
                    'is_day': current_weather.get('is_day')
                },
                'source': 'enhanced-weather-producer',
                'exercice': 6,
                'geocoding_source': 'open-meteo-geocoding'
            }
            
            return enhanced_weather
            
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de l'appel API meteo: {e}")
            return None
        except Exception as e:
            print(f"Erreur lors du traitement des donnees meteo: {e}")
            return None
    
    def send_weather_data(self, weather_data, topic='weather_stream'):
        """Envoie les donnees meteo enrichies vers Kafka"""
        try:
            # Cle de partitionnement basee sur pays/ville pour HDFS
            location = weather_data['location']
            partition_key = f"{location['country_code']}/{location['city']}"
            
            future = self.producer.send(topic, key=partition_key, value=weather_data)
            record_metadata = future.get(timeout=10)
            
            print(f"Donnees envoyees vers Kafka:")
            print(f"  Topic: {record_metadata.topic}")
            print(f"  Partition: {record_metadata.partition}")
            print(f"  Offset: {record_metadata.offset}")
            print(f"  Cle de partitionnement: {partition_key}")
            print(f"  Ville: {location['city']}, {location['country']}")
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
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python enhanced_weather_producer.py <ville> [pays]")
        print("Exemples:")
        print("  python enhanced_weather_producer.py Paris")
        print("  python enhanced_weather_producer.py Paris France")
        print("  python enhanced_weather_producer.py London UK")
        print("  python enhanced_weather_producer.py 'New York' USA")
        print("  python enhanced_weather_producer.py Tokyo Japan")
        sys.exit(1)
    
    city = sys.argv[1]
    country = sys.argv[2] if len(sys.argv) == 3 else None
    
    print(f"=== PRODUCTEUR METEO ETENDU - EXERCICE 6 ===")
    print(f"Ville: {city}")
    print(f"Pays: {country if country else 'Non specifie'}")
    print(f"Topic Kafka: weather_stream")
    print("-" * 60)
    
    producer = None
    try:
        # Creation du producteur
        producer = EnhancedWeatherProducer()
        
        # Etape 1: Geocodage de la ville
        location_info = producer.geocode_location(city, country)
        
        if not location_info:
            print("Impossible de geocoder la localisation")
            sys.exit(1)
        
        # Etape 2: Recuperation des donnees meteo
        weather_data = producer.get_weather_data(location_info)
        
        if not weather_data:
            print("Impossible de recuperer les donnees meteo")
            sys.exit(1)
        
        print("\nDonnees meteo enrichies:")
        print(json.dumps(weather_data, indent=2, ensure_ascii=False))
        
        # Etape 3: Envoi vers Kafka
        print(f"\nEnvoi vers le topic weather_stream...")
        if producer.send_weather_data(weather_data):
            print("\nStreaming meteo enrichi reussi!")
        else:
            print("\nEchec de l'envoi vers Kafka")
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
