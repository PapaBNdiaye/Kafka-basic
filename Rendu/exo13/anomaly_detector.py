#!/usr/bin/env python3
"""
Exercice 13 : Détection d'anomalies climatiques (Batch vs Speed)
- Lire en temps réel depuis Kafka : weather_transformed
- Charger depuis HDFS les profils saisonniers enrichis (par {country, city, month})
- Jointure Batch vs Speed : associer chaque mesure temps réel à la référence historique
- Clé = {country, city, month}
- Colonnes de référence : temp_mean, wind_mean, alert_probabilities
- Définition des seuils d'anomalie :
  - Température : anomalie si |temp_realtime - temp_mean| > 5°C
  - Vent : anomalie si windspeed_realtime > moyenne + 2 std
  - Alertes : écart significatif entre alertes réelles et probabilité historique
- Streaming avec Spark : calcul des écarts en temps réel
- Ajouter champs : is_anomaly, anomaly_type, event_time, city, country, variable, observed_value, expected_value
- Publication : émettre dans Kafka weather_anomalies et sauvegarder dans HDFS
"""

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
import json
import logging
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AnomalyDetector:
    def __init__(self):
        # Configuration Spark
        self.spark = SparkSession.builder \
            .appName("AnomalyDetector") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .getOrCreate()
        
        # Configuration Kafka
        self.kafka_bootstrap_servers = "localhost:9092"
        self.kafka_input_topic = "weather_transformed"
        self.kafka_output_topic = "weather_anomalies"
        
        # Configuration HDFS
        self.hdfs_base_path = "/hdfs-data"
        
        # Seuils d'anomalie
        self.temp_anomaly_threshold = 5.0  # °C
        self.wind_std_multiplier = 2.0  # nombre d'écarts-types
        self.alert_probability_threshold = 0.3  # écart significatif
        
        # Cache pour les profils saisonniers
        self.seasonal_profiles_cache = {}
        
        # Initialisation
        self.load_seasonal_profiles_from_hdfs()

    def load_seasonal_profiles_from_hdfs(self):
        """Charge les profils saisonniers enrichis depuis HDFS"""
        try:
            logger.info("Chargement des profils saisonniers enrichis depuis HDFS...")
            
            # Schéma pour les profils enrichis
            profile_schema = StructType([
                StructField("city", StringType(), True),
                StructField("country", StringType(), True),
                StructField("profile_year", IntegerType(), True),
                StructField("created_at", StringType(), True),
                StructField("monthly_profiles", ArrayType(StructType([
                    StructField("month", IntegerType(), True),
                    StructField("days_count", IntegerType(), True),
                    StructField("temp_mean", DoubleType(), True),
                    StructField("wind_mean", DoubleType(), True),
                    StructField("precip_mean", DoubleType(), True),
                    StructField("temp_std", DoubleType(), True),
                    StructField("wind_std", DoubleType(), True),
                    StructField("temp_min", DoubleType(), True),
                    StructField("temp_max", DoubleType(), True),
                    StructField("wind_min", DoubleType(), True),
                    StructField("wind_max", DoubleType(), True),
                    StructField("temp_median", DoubleType(), True),
                    StructField("wind_median", DoubleType(), True),
                    StructField("temp_q25", DoubleType(), True),
                    StructField("temp_q75", DoubleType(), True),
                    StructField("wind_q25", DoubleType(), True),
                    StructField("wind_q75", DoubleType(), True),
                    StructField("alert_probability_level_1", DoubleType(), True),
                    StructField("alert_probability_level_2", DoubleType(), True)
                ])), True),
                StructField("validation", StructType([
                    StructField("completeness", BooleanType(), True),
                    StructField("total_months", IntegerType(), True),
                    StructField("missing_months", ArrayType(IntegerType()), True)
                ]), True)
            ])
            
            # Lecture des profils enrichis
            hdfs_path = f"{self.hdfs_base_path}/*/*/seasonal_profile_enriched/*/profile.json"
            
            profiles_df = self.spark.read \
                .option("multiline", "true") \
                .schema(profile_schema) \
                .json(hdfs_path)
            
            # Construction du cache
            profiles = profiles_df.collect()
            for profile in profiles:
                city = profile["city"]
                country = profile["country"]
                key = f"{country}_{city}"
                
                # Indexation par mois
                monthly_data = {}
                for month_profile in profile["monthly_profiles"]:
                    month = month_profile["month"]
                    monthly_data[month] = {
                        "temp_mean": month_profile["temp_mean"],
                        "wind_mean": month_profile["wind_mean"],
                        "wind_std": month_profile["wind_std"],
                        "alert_probability_level_1": month_profile["alert_probability_level_1"],
                        "alert_probability_level_2": month_profile["alert_probability_level_2"]
                    }
                
                self.seasonal_profiles_cache[key] = monthly_data
            
            logger.info(f"Profils saisonniers chargés pour {len(self.seasonal_profiles_cache)} villes")
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement des profils saisonniers: {e}")

    def get_weather_schema(self):
        """Retourne le schéma pour les données météo temps réel"""
        return StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("windspeed", DoubleType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("weathercode", IntegerType(), True),
            StructField("alert_level", IntegerType(), True)  # 0, 1, ou 2
        ])

    def detect_temperature_anomaly(self, realtime_temp, reference_profile):
        """Détecte une anomalie de température"""
        if not reference_profile or "temp_mean" not in reference_profile:
            return False, None, None
        
        temp_mean = reference_profile["temp_mean"]
        temp_diff = abs(realtime_temp - temp_mean)
        
        is_anomaly = temp_diff > self.temp_anomaly_threshold
        
        if is_anomaly:
            return True, "temperature", {
                "observed_value": realtime_temp,
                "expected_value": temp_mean,
                "deviation": temp_diff,
                "threshold": self.temp_anomaly_threshold
            }
        
        return False, None, None

    def detect_wind_anomaly(self, realtime_wind, reference_profile):
        """Détecte une anomalie de vent"""
        if not reference_profile or "wind_mean" not in reference_profile or "wind_std" not in reference_profile:
            return False, None, None
        
        wind_mean = reference_profile["wind_mean"]
        wind_std = reference_profile["wind_std"]
        
        # Seuil : moyenne + 2 * écart-type
        wind_threshold = wind_mean + (self.wind_std_multiplier * wind_std)
        
        is_anomaly = realtime_wind > wind_threshold
        
        if is_anomaly:
            return True, "wind", {
                "observed_value": realtime_wind,
                "expected_value": wind_mean,
                "threshold": wind_threshold,
                "deviation": realtime_wind - wind_mean
            }
        
        return False, None, None

    def detect_alert_anomaly(self, realtime_alert_level, reference_profile):
        """Détecte une anomalie d'alerte météo"""
        if not reference_profile or realtime_alert_level not in [1, 2]:
            return False, None, None
        
        # Récupération de la probabilité historique correspondante
        if realtime_alert_level == 1:
            historical_prob = reference_profile.get("alert_probability_level_1", 0.0)
        else:  # alert_level == 2
            historical_prob = reference_profile.get("alert_probability_level_2", 0.0)
        
        # Écart significatif : différence > 30%
        is_anomaly = abs(1.0 - historical_prob) > self.alert_probability_threshold
        
        if is_anomaly:
            return True, "alert", {
                "observed_value": 1.0,  # Présence d'alerte
                "expected_value": historical_prob,
                "alert_level": realtime_alert_level,
                "deviation": abs(1.0 - historical_prob)
            }
        
        return False, None, None

    def process_realtime_data(self, df, epoch_id):
        """Traite les données temps réel pour détecter les anomalies"""
        try:
            if df.count() == 0:
                return
            
            logger.info(f"Traitement de {df.count()} messages temps réel")
            
            # Conversion en DataFrame avec schéma
            weather_schema = self.get_weather_schema()
            processed_df = df.select(
                from_json(col("value").cast("string"), weather_schema).alias("data")
            ).select("data.*")
            
            # Traitement de chaque message
            anomalies = []
            
            for row in processed_df.collect():
                city = row["city"]
                country = row["country"]
                timestamp = row["timestamp"]
                temperature = row["temperature"]
                windspeed = row["windspeed"]
                alert_level = row.get("alert_level", 0)
                
                # Extraction du mois depuis le timestamp
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    month = dt.month
                except:
                    continue
                
                # Récupération du profil de référence
                cache_key = f"{country}_{city}"
                reference_profile = None
                
                if cache_key in self.seasonal_profiles_cache:
                    monthly_profiles = self.seasonal_profiles_cache[cache_key]
                    reference_profile = monthly_profiles.get(month)
                
                if not reference_profile:
                    logger.warning(f"Profil de référence non trouvé pour {city}, {country}, mois {month}")
                    continue
                
                # Détection des anomalies
                anomaly_detected = False
                anomaly_details = []
                
                # Anomalie de température
                temp_anomaly, temp_type, temp_details = self.detect_temperature_anomaly(
                    temperature, reference_profile
                )
                if temp_anomaly:
                    anomaly_detected = True
                    anomaly_details.append({
                        "type": temp_type,
                        "details": temp_details
                    })
                
                # Anomalie de vent
                wind_anomaly, wind_type, wind_details = self.detect_wind_anomaly(
                    windspeed, reference_profile
                )
                if wind_anomaly:
                    anomaly_detected = True
                    anomaly_details.append({
                        "type": wind_type,
                        "details": wind_details
                    })
                
                # Anomalie d'alerte
                if alert_level > 0:
                    alert_anomaly, alert_type, alert_details = self.detect_alert_anomaly(
                        alert_level, reference_profile
                    )
                    if alert_anomaly:
                        anomaly_detected = True
                        anomaly_details.append({
                            "type": alert_type,
                            "details": alert_details
                        })
                
                # Création de l'enregistrement d'anomalie
                if anomaly_detected:
                    anomaly_record = {
                        "event_time": timestamp,
                        "city": city,
                        "country": country,
                        "month": month,
                        "is_anomaly": True,
                        "anomaly_count": len(anomaly_details),
                        "anomaly_types": [detail["type"] for detail in anomaly_details],
                        "anomaly_details": anomaly_details,
                        "original_data": {
                            "temperature": temperature,
                            "windspeed": windspeed,
                            "precipitation": row.get("precipitation", 0.0),
                            "weathercode": row.get("weathercode", 0),
                            "alert_level": alert_level
                        },
                        "reference_profile": {
                            "temp_mean": reference_profile.get("temp_mean", 0.0),
                            "wind_mean": reference_profile.get("wind_mean", 0.0),
                            "wind_std": reference_profile.get("wind_std", 0.0),
                            "alert_prob_level_1": reference_profile.get("alert_probability_level_1", 0.0),
                            "alert_prob_level_2": reference_profile.get("alert_probability_level_2", 0.0)
                        },
                        "detection_timestamp": datetime.now().isoformat()
                    }
                    
                    anomalies.append(anomaly_record)
            
            # Envoi des anomalies vers Kafka
            if anomalies:
                self.send_anomalies_to_kafka(anomalies)
                self.save_anomalies_to_hdfs(anomalies)
                logger.info(f"{len(anomalies)} anomalies détectées et traitées")
            
        except Exception as e:
            logger.error(f"Erreur lors du traitement des données temps réel: {e}")

    def send_anomalies_to_kafka(self, anomalies):
        """Envoie les anomalies vers Kafka"""
        try:
            from kafka import KafkaProducer
            
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            for anomaly in anomalies:
                producer.send(self.kafka_output_topic, anomaly)
            
            producer.flush()
            producer.close()
            
            logger.info(f"Anomalies envoyées vers Kafka topic: {self.kafka_output_topic}")
            
        except Exception as e:
            logger.error(f"Erreur lors de l'envoi vers Kafka: {e}")

    def save_anomalies_to_hdfs(self, anomalies):
        """Sauvegarde les anomalies dans HDFS"""
        try:
            import subprocess
            import os
            
            # Groupement par ville et mois
            anomalies_by_location = {}
            for anomaly in anomalies:
                city = anomaly["city"]
                country = anomaly["country"]
                month = anomaly["month"]
                year = datetime.now().year
                
                key = f"{country}_{city}_{year}_{month}"
                if key not in anomalies_by_location:
                    anomalies_by_location[key] = []
                anomalies_by_location[key].append(anomaly)
            
            # Sauvegarde par groupe
            for key, anomaly_group in anomalies_by_location.items():
                parts = key.split("_")
                country = parts[0]
                city = parts[1]
                year = parts[2]
                month = parts[3]
                
                # Chemin HDFS
                hdfs_path = f"/hdfs-data/{country}/{city}/anomalies/{year}/{month}"
                
                # Création du répertoire
                self.create_hdfs_directory(hdfs_path)
                
                # Sauvegarde
                filename = f"anomalies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                file_path = f"{hdfs_path}/{filename}"
                
                # Conversion en DataFrame et sauvegarde
                anomalies_df = self.spark.createDataFrame(anomaly_group)
                anomalies_df.write \
                    .mode("append") \
                    .option("multiline", "true") \
                    .json(file_path)
                
                logger.info(f"Anomalies sauvegardées: {file_path}")
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde HDFS: {e}")

    def create_hdfs_directory(self, hdfs_path):
        """Crée un répertoire HDFS"""
        try:
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

    def start_streaming(self):
        """Démarre le streaming pour la détection d'anomalies"""
        try:
            logger.info("Démarrage du streaming pour la détection d'anomalies...")
            
            # Lecture depuis Kafka
            kafka_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", self.kafka_input_topic) \
                .option("startingOffsets", "latest") \
                .load()
            
            # Traitement en streaming
            query = kafka_df.writeStream \
                .foreachBatch(self.process_realtime_data) \
                .option("checkpointLocation", "/tmp/anomaly-detector-checkpoint") \
                .trigger(processingTime='10 seconds') \
                .start()
            
            logger.info("Streaming démarré - Détection d'anomalies en cours...")
            logger.info("Appuyez sur Ctrl+C pour arrêter")
            
            # Attente de l'arrêt
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Erreur lors du démarrage du streaming: {e}")

    def close(self):
        """Ferme la session Spark"""
        if self.spark:
            self.spark.stop()
            logger.info("Session Spark fermée")

def main():
    """Fonction principale"""
    print("=== EXERCICE 13: DETECTION D'ANOMALIES CLIMATIQUES ===")
    print("Streaming Spark pour détecter les anomalies en temps réel")
    print("Jointure Batch vs Speed avec les profils saisonniers enrichis")
    print()
    
    detector = AnomalyDetector()
    
    try:
        # Démarrage du streaming
        detector.start_streaming()
        
    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur")
    except Exception as e:
        print(f"Erreur inattendue: {e}")
    finally:
        detector.close()

if __name__ == "__main__":
    main()
