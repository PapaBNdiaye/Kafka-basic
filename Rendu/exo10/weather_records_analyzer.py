#!/usr/bin/env python3
"""
Exercice 10 : Détection des records climatiques locaux
- Job Spark qui analyse HDFS pour chaque ville
- Détecte les records : jour le plus chaud/froid, rafale de vent la plus forte, période la plus pluvieuse
- Émet les records dans Kafka : weather_records
- Sauvegarde dans HDFS : /hdfs-data/{country}/{city}/weather_records
"""

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherRecordsAnalyzer:
    def __init__(self):
        # Configuration Spark
        self.spark = SparkSession.builder \
            .appName("WeatherRecordsAnalyzer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Configuration Kafka
        self.kafka_bootstrap_servers = ['localhost:9092']
        self.kafka_topic = 'weather_records'
        
        # Configuration HDFS
        self.hdfs_base_path = "/hdfs-data"
        self.hdfs_output_path = "/hdfs-data/{country}/{city}/weather_records"
        
        # Schéma pour les données météo
        self.weather_schema = StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("retrieval_date", StringType(), True),
            StructField("period_start", StringType(), True),
            StructField("period_end", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("daily", StructType([
                StructField("time", ArrayType(StringType()), True),
                StructField("temperature_2m_max", ArrayType(DoubleType()), True),
                StructField("temperature_2m_min", ArrayType(DoubleType()), True),
                StructField("precipitation_sum", ArrayType(DoubleType()), True),
                StructField("windspeed_10m_max", ArrayType(DoubleType()), True),
                StructField("weathercode", ArrayType(IntegerType()), True)
            ]), True)
        ])
        
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

    def load_historical_data_from_hdfs(self):
        """Charge les données historiques depuis HDFS"""
        try:
            logger.info("Chargement des données historiques depuis HDFS...")
            
            # Lecture des fichiers JSON depuis HDFS
            hdfs_input_path = f"{self.hdfs_base_path}/*/*/weather_history_raw/*.json"
            
            df = self.spark.read \
                .option("multiline", "true") \
                .schema(self.weather_schema) \
                .json(hdfs_input_path)
            
            logger.info(f"Données chargées depuis HDFS: {df.count()} fichiers")
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement des données HDFS: {e}")
            return None

    def explode_daily_data(self, df):
        """Explose les données quotidiennes en lignes individuelles"""
        try:
            logger.info("Explosion des données quotidiennes...")
            
            # Explosion des données daily
            exploded_df = df.select(
                "city",
                "country", 
                "year",
                explode(
                    arrays_zip(
                        col("daily.time").alias("date"),
                        col("daily.temperature_2m_max").alias("temp_max"),
                        col("daily.temperature_2m_min").alias("temp_min"),
                        col("daily.precipitation_sum").alias("precipitation"),
                        col("daily.windspeed_10m_max").alias("windspeed_max"),
                        col("daily.weathercode").alias("weathercode")
                    )
                ).alias("daily_data")
            ).select(
                "city",
                "country",
                "year", 
                col("daily_data.date").alias("date"),
                col("daily_data.temp_max").alias("temp_max"),
                col("daily_data.temp_min").alias("temp_min"),
                col("daily_data.precipitation").alias("precipitation"),
                col("daily_data.windspeed_max").alias("windspeed_max"),
                col("daily_data.weathercode").alias("weathercode")
            ).filter(
                col("date").isNotNull() &
                col("temp_max").isNotNull() &
                col("temp_min").isNotNull()
            )
            
            logger.info(f"Données explosées: {exploded_df.count()} enregistrements")
            return exploded_df
            
        except Exception as e:
            logger.error(f"Erreur lors de l'explosion des données: {e}")
            return None

    def calculate_climate_records(self, df):
        """Calcule les records climatiques pour chaque ville"""
        try:
            logger.info("Calcul des records climatiques...")
            
            # Records par ville
            records_by_city = []
            
            cities = df.select("city", "country").distinct().collect()
            
            for city_row in cities:
                city = city_row["city"]
                country = city_row["country"]
                
                logger.info(f"Traitement des records pour {city}, {country}")
                
                # Filtrage des données pour cette ville
                city_data = df.filter(
                    (col("city") == city) & (col("country") == country)
                )
                
                # Calcul des records
                records = self.calculate_city_records(city_data, city, country)
                if records:
                    records_by_city.extend(records)
            
            logger.info(f"Records calculés pour {len(records_by_city)} villes")
            return records_by_city
            
        except Exception as e:
            logger.error(f"Erreur lors du calcul des records: {e}")
            return []

    def calculate_city_records(self, city_data, city, country):
        """Calcule les records pour une ville spécifique"""
        try:
            records = []
            
            # Record de température maximale
            temp_max_record = city_data.orderBy(desc("temp_max")).limit(1).collect()
            if temp_max_record:
                record = temp_max_record[0]
                records.append({
                    "city": city,
                    "country": country,
                    "record_type": "temperature_max",
                    "value": float(record["temp_max"]),
                    "date": record["date"],
                    "year": record["year"],
                    "description": f"Jour le plus chaud: {record['temp_max']:.1f}°C le {record['date']}"
                })
            
            # Record de température minimale
            temp_min_record = city_data.orderBy(asc("temp_min")).limit(1).collect()
            if temp_min_record:
                record = temp_min_record[0]
                records.append({
                    "city": city,
                    "country": country,
                    "record_type": "temperature_min",
                    "value": float(record["temp_min"]),
                    "date": record["date"],
                    "year": record["year"],
                    "description": f"Jour le plus froid: {record['temp_min']:.1f}°C le {record['date']}"
                })
            
            # Record de vent maximal
            wind_record = city_data.orderBy(desc("windspeed_max")).limit(1).collect()
            if wind_record:
                record = wind_record[0]
                records.append({
                    "city": city,
                    "country": country,
                    "record_type": "windspeed_max",
                    "value": float(record["windspeed_max"]),
                    "date": record["date"],
                    "year": record["year"],
                    "description": f"Rafale de vent la plus forte: {record['windspeed_max']:.1f} km/h le {record['date']}"
                })
            
            # Record de précipitations maximales
            precip_record = city_data.orderBy(desc("precipitation")).limit(1).collect()
            if precip_record:
                record = precip_record[0]
                records.append({
                    "city": city,
                    "country": country,
                    "record_type": "precipitation_max",
                    "value": float(record["precipitation"]),
                    "date": record["date"],
                    "year": record["year"],
                    "description": f"Jour le plus pluvieux: {record['precipitation']:.1f} mm le {record['date']}"
                })
            
            # Statistiques générales
            stats = city_data.agg(
                count("*").alias("total_days"),
                avg("temp_max").alias("avg_temp_max"),
                avg("temp_min").alias("avg_temp_min"),
                avg("windspeed_max").alias("avg_windspeed"),
                avg("precipitation").alias("avg_precipitation")
            ).collect()
            
            if stats:
                stat = stats[0]
                records.append({
                    "city": city,
                    "country": country,
                    "record_type": "statistics",
                    "total_days": stat["total_days"],
                    "avg_temp_max": float(stat["avg_temp_max"]) if stat["avg_temp_max"] else 0.0,
                    "avg_temp_min": float(stat["avg_temp_min"]) if stat["avg_temp_min"] else 0.0,
                    "avg_windspeed": float(stat["avg_windspeed"]) if stat["avg_windspeed"] else 0.0,
                    "avg_precipitation": float(stat["avg_precipitation"]) if stat["avg_precipitation"] else 0.0,
                    "description": f"Statistiques sur {stat['total_days']} jours de données"
                })
            
            return records
            
        except Exception as e:
            logger.error(f"Erreur lors du calcul des records pour {city}: {e}")
            return []

    def send_records_to_kafka(self, records):
        """Envoie les records vers Kafka"""
        try:
            if not self.producer:
                logger.error("Producteur Kafka non initialisé")
                return
            
            logger.info(f"Envoi de {len(records)} records vers Kafka...")
            
            for record in records:
                # Ajout de métadonnées
                record["timestamp"] = datetime.now().isoformat()
                record["source"] = "weather_records_analyzer"
                
                # Envoi vers Kafka
                future = self.producer.send(self.kafka_topic, record)
                future.get(timeout=10)
                
                logger.debug(f"Record envoyé vers Kafka: {record['record_type']} pour {record['city']}")
            
            logger.info("Tous les records envoyés vers Kafka")
            
        except KafkaError as e:
            logger.error(f"Erreur lors de l'envoi vers Kafka: {e}")
        except Exception as e:
            logger.error(f"Erreur inattendue lors de l'envoi vers Kafka: {e}")

    def save_records_to_hdfs(self, records):
        """Sauvegarde les records dans HDFS"""
        try:
            logger.info("Sauvegarde des records dans HDFS...")
            
            # Groupement par ville
            records_by_city = {}
            for record in records:
                city = record["city"]
                country = record["country"]
                key = f"{country}_{city}"
                
                if key not in records_by_city:
                    records_by_city[key] = {
                        "city": city,
                        "country": country,
                        "records": []
                    }
                
                records_by_city[key]["records"].append(record)
            
            # Sauvegarde par ville
            for city_key, city_data in records_by_city.items():
                city = city_data["city"]
                country = city_data["country"]
                
                # Construction du chemin HDFS
                hdfs_path = f"/hdfs-data/{country}/{city}/weather_records"
                
                # Création du répertoire
                self.create_hdfs_directory(hdfs_path)
                
                # Sauvegarde du fichier
                filename = f"climate_records_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                file_path = f"{hdfs_path}/{filename}"
                
                # Conversion en DataFrame et sauvegarde
                records_df = self.spark.createDataFrame(city_data["records"])
                records_df.write \
                    .mode("overwrite") \
                    .option("multiline", "true") \
                    .json(file_path)
                
                logger.info(f"Records sauvegardés pour {city}: {file_path}")
            
            logger.info("Sauvegarde HDFS terminée")
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde HDFS: {e}")

    def create_hdfs_directory(self, hdfs_path):
        """Crée un répertoire HDFS"""
        try:
            # Utilisation de la commande hdfs via subprocess
            import subprocess
            import os
            
            # Ajout de Hadoop au PATH si nécessaire
            hadoop_home = os.environ.get('HADOOP_HOME', 'C:\\hadoop')
            hadoop_bin = os.path.join(hadoop_home, 'bin')
            
            if hadoop_bin not in os.environ.get('PATH', ''):
                os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
            
            # Création du répertoire
            result = subprocess.run(
                ["hdfs", "dfs", "-mkdir", "-p", hdfs_path],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                logger.debug(f"Répertoire HDFS créé: {hdfs_path}")
            else:
                logger.warning(f"Impossible de créer le répertoire HDFS {hdfs_path}: {result.stderr}")
                
        except Exception as e:
            logger.warning(f"Erreur lors de la création du répertoire HDFS {hdfs_path}: {e}")

    def run_analysis(self):
        """Exécute l'analyse complète des records climatiques"""
        try:
            logger.info("Début de l'analyse des records climatiques")
            
            # 1. Chargement des données depuis HDFS
            historical_df = self.load_historical_data_from_hdfs()
            if not historical_df:
                logger.error("Impossible de charger les données historiques")
                return False
            
            # 2. Explosion des données quotidiennes
            exploded_df = self.explode_daily_data(historical_df)
            if not exploded_df:
                logger.error("Impossible d'exploser les données quotidiennes")
                return False
            
            # 3. Calcul des records climatiques
            records = self.calculate_climate_records(exploded_df)
            if not records:
                logger.error("Aucun record calculé")
                return False
            
            # 4. Envoi vers Kafka
            self.send_records_to_kafka(records)
            
            # 5. Sauvegarde dans HDFS
            self.save_records_to_hdfs(records)
            
            logger.info(f"Analyse terminée: {len(records)} records traités")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de l'analyse: {e}")
            return False
        finally:
            # Affichage des résultats
            self.display_results(records)

    def display_results(self, records):
        """Affiche un résumé des résultats"""
        if not records:
            return
        
        print("\n=== RESULTATS DE L'ANALYSE DES RECORDS CLIMATIQUES ===")
        
        # Groupement par ville
        cities = {}
        for record in records:
            city_key = f"{record['city']}, {record['country']}"
            if city_key not in cities:
                cities[city_key] = []
            cities[city_key].append(record)
        
        for city_key, city_records in cities.items():
            print(f"\n{city_key}:")
            for record in city_records:
                if record['record_type'] != 'statistics':
                    print(f"  - {record['description']}")
        
        print(f"\nTotal: {len(records)} records analysés")
        print("Données disponibles dans:")
        print("- Kafka topic: weather_records")
        print("- HDFS: /hdfs-data/{country}/{city}/weather_records/")

    def close(self):
        """Ferme les connexions"""
        if self.producer:
            self.producer.close()
            logger.info("Producteur Kafka fermé")
        
        if self.spark:
            self.spark.stop()
            logger.info("Session Spark fermée")

def main():
    """Fonction principale"""
    print("=== EXERCICE 10: DETECTION DES RECORDS CLIMATIQUES LOCAUX ===")
    print("Job Spark analysant HDFS pour détecter les records climatiques")
    print("Émission des records dans Kafka et sauvegarde dans HDFS")
    print()
    
    analyzer = WeatherRecordsAnalyzer()
    
    try:
        # Exécution de l'analyse
        success = analyzer.run_analysis()
        
        if success:
            print("\nAnalyse des records climatiques terminée avec succès")
        else:
            print("\nErreur lors de l'analyse des records climatiques")
            
    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur")
    except Exception as e:
        print(f"Erreur inattendue: {e}")
    finally:
        analyzer.close()

if __name__ == "__main__":
    main()
