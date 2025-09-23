#!/usr/bin/env python3
"""
Exercice 5 : Agregats en temps reel avec Spark
Calcule des agregats sur des fenetres glissantes du flux weather_transformed
"""

import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

def create_spark_session():
    """Cree une session Spark pour les agregats"""
    return SparkSession.builder \
        .appName("WeatherAggregator-Exercice5") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoint_aggregates") \
        .getOrCreate()

def define_transformed_schema():
    """Definit le schema des donnees transformees"""
    return StructType([
        StructField("event_time", StringType(), True),
        StructField("processing_timestamp", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timezone", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("winddirection", DoubleType(), True),
        StructField("weathercode", IntegerType(), True),
        StructField("is_day", IntegerType(), True),
        StructField("wind_alert_level", StringType(), True),
        StructField("heat_alert_level", StringType(), True),
        StructField("source", StringType(), True),
        StructField("exercice", IntegerType(), True),
        StructField("original_source", StringType(), True)
    ])

def extract_city_from_coordinates(lat, lon):
    """Extrait une approximation de ville basee sur les coordonnees"""
    # Mapping simple des coordonnees vers les villes connues
    cities = {
        (48.8566, 2.3522): "Paris",
        (43.6043, 1.4437): "Toulouse", 
        (45.764, 4.8357): "Lyon",
        (50.8503, 4.3517): "Bruxelles",
        (40.4168, -3.7038): "Madrid",
        (52.52, 13.405): "Berlin",
        (41.9028, 12.4964): "Rome",
        (37.9838, 23.7275): "Athenes",
        (64.1466, -21.9426): "Reykjavik",
        (43.7102, 7.262): "Nice",
        (59.3293, 18.0686): "Stockholm",
        (37.3886, -5.9823): "Seville",
        (60.1699, 24.9384): "Helsinki"
    }
    
    # Recherche de la ville la plus proche
    for (city_lat, city_lon), city_name in cities.items():
        if abs(lat - city_lat) < 0.1 and abs(lon - city_lon) < 0.1:
            return city_name
    
    return "Ville_Inconnue"

# Enregistrer l'UDF
extract_city_udf = udf(extract_city_from_coordinates, StringType())

def main():
    """Fonction principale pour les agregats"""
    print("=== AGREGATS TEMPS REEL - EXERCICE 5 ===")
    print("Calcul d'agregats sur fenetres glissantes")
    print("Source: weather_transformed")
    print("-" * 60)
    
    # Creation de la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Lecture du flux weather_transformed
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "weather_transformed") \
            .option("startingOffsets", "earliest") \
            .load()
        
        print("Connexion au topic weather_transformed etablie")
        
        # Parsing des donnees JSON
        weather_df = df.select(
            col("key").cast("string").alias("location_key"),
            from_json(col("value").cast("string"), define_transformed_schema()).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select(
            col("location_key"),
            col("kafka_timestamp"),
            to_timestamp(col("data.event_time")).alias("event_time"),
            col("data.latitude").alias("latitude"),
            col("data.longitude").alias("longitude"),
            col("data.timezone").alias("timezone"),
            col("data.temperature").alias("temperature"),
            col("data.windspeed").alias("windspeed"),
            col("data.wind_alert_level").alias("wind_alert_level"),
            col("data.heat_alert_level").alias("heat_alert_level"),
            col("data.source").alias("source"),
            col("data.exercice").alias("exercice")
        ).filter(
            col("exercice") == 4  # Filtrer les donnees de l'exercice 4
        ).withColumn(
            "city", extract_city_udf(col("latitude"), col("longitude"))
        )
        
        print("Pipeline de donnees configure")
        print("Fenetres glissantes: 1 minute avec mise a jour toutes les 30 secondes")
        
        # Agregats sur fenetre glissante de 1 minute
        windowed_aggregates = weather_df \
            .withWatermark("event_time", "2 minutes") \
            .groupBy(
                window(col("event_time"), "1 minute", "30 seconds"),
                col("city")
            ).agg(
                # Metriques de temperature
                avg("temperature").alias("temp_moyenne"),
                min("temperature").alias("temp_min"),
                max("temperature").alias("temp_max"),
                
                # Comptage des alertes vent
                sum(when(col("wind_alert_level") == "level_1", 1).otherwise(0)).alias("vent_level1_count"),
                sum(when(col("wind_alert_level") == "level_2", 1).otherwise(0)).alias("vent_level2_count"),
                
                # Comptage des alertes chaleur
                sum(when(col("heat_alert_level") == "level_1", 1).otherwise(0)).alias("chaleur_level1_count"),
                sum(when(col("heat_alert_level") == "level_2", 1).otherwise(0)).alias("chaleur_level2_count"),
                
                # Total des alertes significatives
                sum(when((col("wind_alert_level").isin("level_1", "level_2")) | 
                        (col("heat_alert_level").isin("level_1", "level_2")), 1).otherwise(0)).alias("total_alertes"),
                
                # Nombre total de mesures
                count("*").alias("nb_mesures")
            )
        
        # Formatage des resultats pour affichage
        formatted_results = windowed_aggregates.select(
            col("window.start").alias("fenetre_debut"),
            col("window.end").alias("fenetre_fin"),
            col("city").alias("ville"),
            round(col("temp_moyenne"), 1).alias("temp_moy"),
            col("temp_min"),
            col("temp_max"),
            col("vent_level1_count").alias("vent_L1"),
            col("vent_level2_count").alias("vent_L2"),
            col("chaleur_level1_count").alias("chaleur_L1"),
            col("chaleur_level2_count").alias("chaleur_L2"),
            col("total_alertes"),
            col("nb_mesures")
        )
        
        print("Configuration des agregats terminee")
        print("\nDemarrage du streaming d'agregats...")
        print("Metriques calculees:")
        print("- Temperature: moyenne, min, max par ville")
        print("- Alertes vent: comptage level_1 et level_2")
        print("- Alertes chaleur: comptage level_1 et level_2") 
        print("- Total alertes significatives par ville")
        print("\nResultats affiches toutes les 30 secondes")
        print("Ctrl+C pour arreter")
        
        # Affichage des resultats en continu
        query = formatted_results \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 20) \
            .trigger(processingTime='30 seconds') \
            .start()
        
        # Attendre l'arret
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nArret demande par l'utilisateur")
    except Exception as e:
        print(f"\nErreur: {e}")
        return 1
    finally:
        spark.stop()
        print("Session Spark fermee")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
