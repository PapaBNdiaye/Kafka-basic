#!/usr/bin/env python3
"""
Exercice 7 : Stockage HDFS avec detection automatique
Utilise HDFS reel si disponible, sinon simule la structure HDFS localement
"""

import sys
import json
import os
from datetime import datetime
from kafka import KafkaConsumer
from pathlib import Path

class HDFSStorage:
    def __init__(self):
        """Initialise le stockage HDFS (reel ou simule)"""
        self.hdfs_available = False
        self.hdfs_client = None
        self.local_hdfs_path = Path("hdfs-data")
        
        # Tentative de connexion HDFS reel
        try:
            from hdfs3 import HDFileSystem
            self.hdfs_client = HDFileSystem(host='localhost', port=9000)
            # Test de connexion
            self.hdfs_client.ls('/')
            self.hdfs_available = True
            print("HDFS reel detecte et connecte")
        except Exception as e:
            print("HDFS non disponible, utilisation simulation locale")
            print(f"Details: {e}")
            self.hdfs_available = False
            self.local_hdfs_path.mkdir(exist_ok=True)
    
    def create_hdfs_path(self, country, city):
        """Cree le chemin HDFS selon la structure demandee"""
        if self.hdfs_available:
            # HDFS reel
            hdfs_dir = f"/hdfs-data/{country}/{city}"
            try:
                self.hdfs_client.makedirs(hdfs_dir, exist_ok=True)
                return f"{hdfs_dir}/alerts.json"
            except Exception as e:
                print(f"Erreur creation repertoire HDFS: {e}")
                return None
        else:
            # Simulation locale
            country_path = self.local_hdfs_path / country
            city_path = country_path / city
            city_path.mkdir(parents=True, exist_ok=True)
            return city_path / "alerts.json"
    
    def read_existing_alerts(self, file_path):
        """Lit les alertes existantes"""
        try:
            if self.hdfs_available:
                # HDFS reel
                if self.hdfs_client.exists(file_path):
                    with self.hdfs_client.open(file_path, 'r') as f:
                        return json.load(f)
            else:
                # Simulation locale
                if Path(file_path).exists():
                    with open(file_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
        except Exception as e:
            print(f"Erreur lecture alertes existantes: {e}")
        
        return []
    
    def write_alerts(self, file_path, alerts_data):
        """Ecrit les alertes dans HDFS"""
        try:
            if self.hdfs_available:
                # HDFS reel
                with self.hdfs_client.open(file_path, 'w') as f:
                    json.dump(alerts_data, f, indent=2, ensure_ascii=False)
            else:
                # Simulation locale
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(alerts_data, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Erreur ecriture HDFS: {e}")
            return False
    
    def list_hdfs_structure(self):
        """Liste la structure HDFS creee"""
        try:
            if self.hdfs_available:
                # HDFS reel
                if self.hdfs_client.exists("/hdfs-data"):
                    return self.hdfs_client.walk("/hdfs-data")
            else:
                # Simulation locale
                if self.local_hdfs_path.exists():
                    result = []
                    for root, dirs, files in os.walk(self.local_hdfs_path):
                        result.append((root, dirs, files))
                    return result
        except Exception as e:
            print(f"Erreur listage HDFS: {e}")
        
        return []

def extract_location_info(message_data):
    """Extrait les informations de localisation"""
    location = message_data.get('location', {})
    
    # Messages enrichis (exercice 6)
    if 'country_code' in location and 'city' in location:
        return {
            'country': location['country_code'],
            'city': location['city'].replace(' ', '_').replace('/', '_'),
            'country_full': location.get('country', 'Unknown')
        }
    
    # Messages avec coordonnees (exercices precedents)
    lat = message_data.get('latitude') or location.get('latitude')
    lon = message_data.get('longitude') or location.get('longitude')
    
    if lat and lon:
        # Mapping coordonnees vers pays/ville
        city_mapping = {
            (48.8566, 2.3522): {"country": "FR", "city": "Paris", "country_full": "France"},
            (43.6043, 1.4437): {"country": "FR", "city": "Toulouse", "country_full": "France"},
            (45.764, 4.8357): {"country": "FR", "city": "Lyon", "country_full": "France"},
            (50.8503, 4.3517): {"country": "BE", "city": "Bruxelles", "country_full": "Belgique"},
            (40.4168, -3.7038): {"country": "ES", "city": "Madrid", "country_full": "Espagne"},
            (52.52, 13.405): {"country": "DE", "city": "Berlin", "country_full": "Allemagne"},
            (41.9028, 12.4964): {"country": "IT", "city": "Rome", "country_full": "Italie"}
        }
        
        for (city_lat, city_lon), city_info in city_mapping.items():
            if abs(lat - city_lat) < 0.1 and abs(lon - city_lon) < 0.1:
                return city_info
    
    return {"country": "UNKNOWN", "city": "Unknown_Location", "country_full": "Inconnu"}

def main():
    """Fonction principale"""
    print("=== STOCKAGE HDFS ORGANISE - EXERCICE 7 ===")
    print("Consommateur weather_transformed -> Structure HDFS")
    print("-" * 60)
    
    # Initialisation du stockage HDFS
    hdfs_storage = HDFSStorage()
    
    try:
        # Configuration du consommateur
        consumer = KafkaConsumer(
            'weather_transformed',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=15000,
            value_deserializer=lambda x: x.decode('utf-8') if x else None
        )
        
        print("Consommateur connecte au topic weather_transformed")
        print("Sauvegarde des alertes dans HDFS...")
        print()
        
        processed_count = 0
        saved_count = 0
        
        for message in consumer:
            processed_count += 1
            
            try:
                data = json.loads(message.value)
                
                # Extraction localisation
                location_info = extract_location_info(data)
                
                # Creation du chemin HDFS
                hdfs_path = hdfs_storage.create_hdfs_path(location_info['country'], location_info['city'])
                
                if hdfs_path:
                    # Lecture des alertes existantes
                    existing_alerts = hdfs_storage.read_existing_alerts(hdfs_path)
                    
                    # Preparation de la nouvelle alerte
                    alert_data = {
                        'timestamp': datetime.now().isoformat(),
                        'event_time': data.get('event_time'),
                        'location': location_info,
                        'weather': {
                            'temperature': data.get('temperature'),
                            'windspeed': data.get('windspeed'),
                            'winddirection': data.get('winddirection')
                        },
                        'alerts': {
                            'wind_level': data.get('wind_alert_level'),
                            'heat_level': data.get('heat_alert_level')
                        },
                        'kafka_metadata': {
                            'offset': message.offset,
                            'partition': message.partition,
                            'source': data.get('source')
                        }
                    }
                    
                    # Ajout a la liste
                    existing_alerts.append(alert_data)
                    
                    # Sauvegarde
                    if hdfs_storage.write_alerts(hdfs_path, existing_alerts):
                        saved_count += 1
                        temp = data.get('temperature', 'N/A')
                        wind = data.get('wind_alert_level', 'N/A')
                        heat = data.get('heat_alert_level', 'N/A')
                        
                        print(f"[{processed_count:2d}] {location_info['country']}/{location_info['city']}")
                        print(f"     {temp}°C, Vent:{wind}, Chaleur:{heat}")
                        print(f"     -> {hdfs_path}")
                        print()
                
            except json.JSONDecodeError:
                print(f"Message non-JSON ignore")
            except Exception as e:
                print(f"Erreur traitement message: {e}")
        
        consumer.close()
        
        print(f"=== SAUVEGARDE HDFS TERMINEE ===")
        print(f"Messages traites: {processed_count}")
        print(f"Alertes sauvegardees: {saved_count}")
        print(f"Type stockage: {'HDFS reel' if hdfs_storage.hdfs_available else 'Simulation locale'}")
        
        # Affichage de la structure
        print(f"\n=== STRUCTURE HDFS ===")
        structure = hdfs_storage.list_hdfs_structure()
        
        if structure:
            for root, dirs, files in structure:
                level = root.replace(str(hdfs_storage.local_hdfs_path), '').count(os.sep)
                indent = ' ' * 2 * level
                print(f"{indent}{os.path.basename(root)}/")
                subindent = ' ' * 2 * (level + 1)
                for file in files:
                    if file.endswith('.json'):
                        file_path = Path(root) / file
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                alerts = json.load(f)
                                print(f"{subindent}{file} ({len(alerts)} alertes)")
                        except:
                            print(f"{subindent}{file}")
        
    except KeyboardInterrupt:
        print(f"\nArret demande. Messages traites: {processed_count}")
    except Exception as e:
        print(f"Erreur: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
