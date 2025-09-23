#!/usr/bin/env python3
"""
Exercice 4 : Transformation des données et détection d'alertes avec Spark
Traite le flux weather_stream en temps réel et produit le topic weather_transformed
avec les alertes de vent et de chaleur
"""

import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    """Crée une session Spark avec les configurations nécessaires"""
    return SparkSession.builder \
        .appName("WeatherTransformer-Exercice4") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoint") \
        .getOrCreate()

def define_weather_schema():
    """Définit le schéma des données météo"""
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("location", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("timezone", StringType(), True)
        ]), True),
        StructField("current_weather", StructType([
            StructField("time", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("windspeed", DoubleType(), True),
            StructField("winddirection", DoubleType(), True),
            StructField("weathercode", IntegerType(), True),
            StructField("is_day", IntegerType(), True)
        ]), True),
        StructField("source", StringType(), True),
        StructField("exercice", IntegerType(), True)
    ])

def wind_alert_level(windspeed):
    """Détermine le niveau d'alerte vent"""
    if windspeed < 10:
        return "level_0"  # Vent faible
    elif windspeed <= 20:
        return "level_1"  # Vent modéré
    else:
        return "level_2"  # Vent fort

def heat_alert_level(temperature):
    """Détermine le niveau d'alerte chaleur"""
    if temperature < 25:
        return "level_0"  # Température normale
    elif temperature <= 35:
        return "level_1"  # Chaleur modérée
    else:
        return "level_2"  # Canicule

# Enregistrement des UDF (User Defined Functions)
wind_alert_udf = udf(wind_alert_level, StringType())
heat_alert_udf = udf(heat_alert_level, StringType())

def transform_weather_data(df):
    """Transforme les données météo et ajoute les alertes"""
    
    # Extraction des données météo depuis le JSON
    weather_df = df.select(
        col("key").cast("string").alias("location_key"),
        from_json(col("value").cast("string"), define_weather_schema()).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        col("location_key"),
        col("kafka_timestamp"),
        col("data.timestamp").alias("original_timestamp"),
        col("data.location.latitude").alias("latitude"),
        col("data.location.longitude").alias("longitude"),
        col("data.location.timezone").alias("timezone"),
        col("data.current_weather.temperature").alias("temperature"),
        col("data.current_weather.windspeed").alias("windspeed"),
        col("data.current_weather.winddirection").alias("winddirection"),
        col("data.current_weather.weathercode").alias("weathercode"),
        col("data.current_weather.is_day").alias("is_day"),
        col("data.source").alias("source"),
        col("data.exercice").alias("exercice")
    ).filter(
        col("exercice") == 3  # Filtrer seulement les données de l'exercice 3
    )
    
    # Ajout des colonnes transformées
    transformed_df = weather_df.withColumn(
        "event_time", current_timestamp()
    ).withColumn(
        "wind_alert_level", wind_alert_udf(col("windspeed"))
    ).withColumn(
        "heat_alert_level", heat_alert_udf(col("temperature"))
    ).withColumn(
        "processing_timestamp", lit(datetime.now().isoformat())
    ).withColumn(
        "exercice", lit(4)  # Marquer comme exercice 4
    )
    
    return transformed_df

def main():
    """Fonction principale"""
    print("=== SPARK STREAMING - EXERCICE 4 ===")
    print("Transformation des données météo et détection d'alertes")
    print("Source: weather_stream")
    print("Destination: weather_transformed")
    print("-" * 60)
    
    # Création de la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Réduire les logs
    
    try:
        # Lecture du flux Kafka weather_stream
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "weather_stream") \
            .option("startingOffsets", "latest") \
            .load()
        
        print("[OK] Connexion au topic weather_stream etablie")
        
        # Transformation des données
        transformed_df = transform_weather_data(df)
        
        print("[OK] Pipeline de transformation configure")
        print("\nAlertes configurées:")
        print("  Vent: <10 m/s → level_0, 10-20 m/s → level_1, >20 m/s → level_2")
        print("  Chaleur: <25°C → level_0, 25-35°C → level_1, >35°C → level_2")
        
        # Création du topic weather_transformed
        print("\n[OK] Creation du topic weather_transformed")
        
        # Préparation des données pour Kafka
        kafka_output = transformed_df.select(
            col("location_key").alias("key"),
            to_json(struct(
                col("event_time"),
                col("processing_timestamp"),
                col("latitude"),
                col("longitude"),
                col("timezone"),
                col("temperature"),
                col("windspeed"),
                col("winddirection"),
                col("weathercode"),
                col("is_day"),
                col("wind_alert_level"),
                col("heat_alert_level"),
                col("source"),
                col("exercice")
            )).alias("value")
        )
        
        # Écriture vers le topic weather_transformed
        query = kafka_output \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "weather_transformed") \
            .option("checkpointLocation", "./checkpoint") \
            .outputMode("append") \
            .trigger(processingTime='10 seconds') \
            .start()
        
        print("[OK] Streaming vers weather_transformed demarre")
        print("\n[PROCESSING] Traitement en cours (Ctrl+C pour arreter)")
        print("Envoyez des données météo avec l'exercice 3 pour voir les transformations")
        
        # Attendre l'arrêt du streaming
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n[STOP] Arret demande par l'utilisateur")
    except Exception as e:
        print(f"\n[ERR] Erreur: {e}")
        return 1
    finally:
        spark.stop()
        print("[OK] Session Spark fermee")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
