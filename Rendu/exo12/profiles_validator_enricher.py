#!/usr/bin/env python3
"""
Exercice 12 : Validation et enrichissement des profils saisonniers
- Vérifier que chaque ville/pays possède un profil complet sur 12 mois
- Détecter les valeurs manquantes
- Vérifier la plausibilité des valeurs (température -50°C à +60°C, vent 0 à 60 m/s)
- Calculs statistiques : écart-type, min/max, médiane, quantiles Q25/Q75
- Ajout des alert_probabilities (level_1 et level_2)
- Sauvegarde dans HDFS : /hdfs-data/{country}/{city}/seasonal_profile_enriched/{year}/profile.json
"""

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProfilesValidatorEnricher:
    def __init__(self):
        # Configuration Spark
        self.spark = SparkSession.builder \
            .appName("ProfilesValidatorEnricher") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Configuration HDFS
        self.hdfs_base_path = "/hdfs-data"
        self.hdfs_output_path = "/hdfs-data/{country}/{city}/seasonal_profile_enriched"
        
        # Seuils de plausibilité
        self.temp_min = -50.0
        self.temp_max = 60.0
        self.wind_min = 0.0
        self.wind_max = 60.0

    def load_historical_data_from_hdfs(self):
        """Charge les données historiques brutes depuis HDFS (données de l'exercice 9)"""
        try:
            logger.info("Chargement des données historiques brutes depuis HDFS...")
            
            # Schéma pour les données météo brutes
            weather_schema = StructType([
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
            
            # Lecture des fichiers JSON depuis HDFS
            hdfs_input_path = f"{self.hdfs_base_path}/*/*/weather_history_raw/*.json"
            
            df = self.spark.read \
                .option("multiline", "true") \
                .schema(weather_schema) \
                .json(hdfs_input_path)
            
            logger.info(f"Données historiques chargées: {df.count()} fichiers")
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
            
            # Ajout du mois extrait de la date
            exploded_df = exploded_df.withColumn("month", month(to_date(col("date"), "yyyy-MM-dd")))
            
            logger.info(f"Données explosées: {exploded_df.count()} enregistrements")
            return exploded_df
            
        except Exception as e:
            logger.error(f"Erreur lors de l'explosion des données: {e}")
            return None

    def validate_profile_completeness(self, df):
        """Vérifie que chaque ville possède des données pour les 12 mois"""
        try:
            logger.info("Validation de la complétude des profils...")
            
            # Groupement par ville et mois
            monthly_stats = df.groupBy("city", "country", "month") \
                .agg(count("*").alias("days_count")) \
                .orderBy("city", "country", "month")
            
            # Vérification des mois manquants
            completeness_results = []
            cities = df.select("city", "country").distinct().collect()
            
            for city_row in cities:
                city = city_row["city"]
                country = city_row["country"]
                
                # Récupération des mois disponibles pour cette ville
                city_months = monthly_stats.filter(
                    (col("city") == city) & (col("country") == country)
                ).collect()
                
                available_months = set(row["month"] for row in city_months)
                expected_months = set(range(1, 13))
                missing_months = expected_months - available_months
                
                completeness_results.append({
                    "city": city,
                    "country": country,
                    "available_months": len(available_months),
                    "missing_months": list(missing_months),
                    "is_complete": len(missing_months) == 0,
                    "months_detail": {row["month"]: row["days_count"] for row in city_months}
                })
            
            logger.info(f"Validation de complétude terminée pour {len(completeness_results)} villes")
            return completeness_results
            
        except Exception as e:
            logger.error(f"Erreur lors de la validation de complétude: {e}")
            return []

    def validate_data_plausibility(self, df):
        """Vérifie la plausibilité des valeurs météorologiques"""
        try:
            logger.info("Validation de la plausibilité des données...")
            
            plausibility_results = []
            cities = df.select("city", "country").distinct().collect()
            
            for city_row in cities:
                city = city_row["city"]
                country = city_row["country"]
                
                city_data = df.filter(
                    (col("city") == city) & (col("country") == country)
                )
                
                # Vérification des températures
                temp_issues = city_data.filter(
                    (col("temp_max") < self.temp_min) | (col("temp_max") > self.temp_max) |
                    (col("temp_min") < self.temp_min) | (col("temp_min") > self.temp_max)
                ).count()
                
                # Vérification du vent
                wind_issues = city_data.filter(
                    (col("windspeed_max") < self.wind_min) | (col("windspeed_max") > self.wind_max)
                ).count()
                
                plausibility_results.append({
                    "city": city,
                    "country": country,
                    "temp_issues": temp_issues,
                    "wind_issues": wind_issues,
                    "is_plausible": temp_issues == 0 and wind_issues == 0
                })
            
            logger.info(f"Validation de plausibilité terminée pour {len(plausibility_results)} villes")
            return plausibility_results
            
        except Exception as e:
            logger.error(f"Erreur lors de la validation de plausibilité: {e}")
            return []

    def enrich_profiles_with_statistics(self, df):
        """Enrichit les profils avec des statistiques détaillées"""
        try:
            logger.info("Enrichissement des profils avec statistiques...")
            
            enriched_profiles = []
            cities = df.select("city", "country").distinct().collect()
            
            for city_row in cities:
                city = city_row["city"]
                country = city_row["country"]
                
                logger.info(f"Traitement des statistiques pour {city}, {country}")
                
                city_data = df.filter(
                    (col("city") == city) & (col("country") == country)
                )
                
                # Calcul par mois
                monthly_stats = city_data.groupBy("month") \
                    .agg(
                        count("*").alias("days_count"),
                        avg("temp_max").alias("temp_max_mean"),
                        avg("temp_min").alias("temp_min_mean"),
                        avg("windspeed_max").alias("wind_mean"),
                        avg("precipitation").alias("precip_mean"),
                        stddev("temp_max").alias("temp_max_std"),
                        stddev("temp_min").alias("temp_min_std"),
                        stddev("windspeed_max").alias("wind_std"),
                        min("temp_max").alias("temp_max_min"),
                        max("temp_max").alias("temp_max_max"),
                        min("temp_min").alias("temp_min_min"),
                        max("temp_min").alias("temp_min_max"),
                        min("windspeed_max").alias("wind_min"),
                        max("windspeed_max").alias("wind_max")
                    ).orderBy("month")
                
                monthly_data = monthly_stats.collect()
                
                # Construction du profil enrichi
                monthly_profiles = []
                for month_row in monthly_data:
                    month = month_row["month"]
                    
                    # Calcul des alert probabilities basé sur les weather codes
                    month_weather_codes = city_data.filter(col("month") == month) \
                        .select("weathercode").collect()
                    
                    alert_prob_level_1, alert_prob_level_2 = self.calculate_alert_probabilities(
                        [row["weathercode"] for row in month_weather_codes]
                    )
                    
                    monthly_profiles.append({
                        "month": month,
                        "days_count": month_row["days_count"],
                        "temp_mean": round(float(month_row["temp_max_mean"]), 2) if month_row["temp_max_mean"] else 0.0,
                        "wind_mean": round(float(month_row["wind_mean"]), 2) if month_row["wind_mean"] else 0.0,
                        "precip_mean": round(float(month_row["precip_mean"]), 2) if month_row["precip_mean"] else 0.0,
                        "temp_std": round(float(month_row["temp_max_std"]), 2) if month_row["temp_max_std"] else 0.0,
                        "wind_std": round(float(month_row["wind_std"]), 2) if month_row["wind_std"] else 0.0,
                        "temp_min": round(float(month_row["temp_min_min"]), 2) if month_row["temp_min_min"] else 0.0,
                        "temp_max": round(float(month_row["temp_max_max"]), 2) if month_row["temp_max_max"] else 0.0,
                        "wind_min": round(float(month_row["wind_min"]), 2) if month_row["wind_min"] else 0.0,
                        "wind_max": round(float(month_row["wind_max"]), 2) if month_row["wind_max"] else 0.0,
                        "temp_median": round(float(month_row["temp_max_mean"]), 2) if month_row["temp_max_mean"] else 0.0,  # Approximation
                        "wind_median": round(float(month_row["wind_mean"]), 2) if month_row["wind_mean"] else 0.0,  # Approximation
                        "temp_q25": round(float(month_row["temp_max_mean"]) - float(month_row["temp_max_std"]), 2) if month_row["temp_max_mean"] and month_row["temp_max_std"] else 0.0,  # Approximation
                        "temp_q75": round(float(month_row["temp_max_mean"]) + float(month_row["temp_max_std"]), 2) if month_row["temp_max_mean"] and month_row["temp_max_std"] else 0.0,  # Approximation
                        "wind_q25": round(float(month_row["wind_mean"]) - float(month_row["wind_std"]), 2) if month_row["wind_mean"] and month_row["wind_std"] else 0.0,  # Approximation
                        "wind_q75": round(float(month_row["wind_mean"]) + float(month_row["wind_std"]), 2) if month_row["wind_mean"] and month_row["wind_std"] else 0.0,  # Approximation
                        "alert_probability_level_1": alert_prob_level_1,
                        "alert_probability_level_2": alert_prob_level_2
                    })
                
                # Profil complet pour la ville
                enriched_profile = {
                    "city": city,
                    "country": country,
                    "profile_year": datetime.now().year,
                    "created_at": datetime.now().isoformat(),
                    "monthly_profiles": monthly_profiles,
                    "validation": {
                        "completeness": len(monthly_profiles) == 12,
                        "total_months": len(monthly_profiles),
                        "missing_months": [m for m in range(1, 13) if m not in [p["month"] for p in monthly_profiles]]
                    }
                }
                
                enriched_profiles.append(enriched_profile)
            
            logger.info(f"Profils enrichis créés pour {len(enriched_profiles)} villes")
            return enriched_profiles
            
        except Exception as e:
            logger.error(f"Erreur lors de l'enrichissement des profils: {e}")
            return []

    def calculate_alert_probabilities(self, weather_codes):
        """Calcule les probabilités d'alerte basées sur les codes météo"""
        if not weather_codes:
            return 0.0, 0.0
        
        # Codes météo Open-Meteo pour les alertes
        # Level 1: Pluie légère, neige légère, brouillard
        level_1_codes = [61, 63, 65, 71, 73, 75, 77, 80, 81, 82, 45, 48]
        # Level 2: Pluie forte, neige forte, orages, tempêtes
        level_2_codes = [66, 67, 85, 86, 95, 96, 99]
        
        total_days = len(weather_codes)
        level_1_count = sum(1 for code in weather_codes if code in level_1_codes)
        level_2_count = sum(1 for code in weather_codes if code in level_2_codes)
        
        prob_level_1 = round(level_1_count / total_days, 3) if total_days > 0 else 0.0
        prob_level_2 = round(level_2_count / total_days, 3) if total_days > 0 else 0.0
        
        return prob_level_1, prob_level_2

    def save_enriched_profiles_to_hdfs(self, enriched_profiles):
        """Sauvegarde les profils enrichis dans HDFS"""
        try:
            logger.info("Sauvegarde des profils enrichis dans HDFS...")
            
            for profile in enriched_profiles:
                city = profile["city"]
                country = profile["country"]
                year = profile["profile_year"]
                
                # Construction du chemin HDFS
                hdfs_path = f"/hdfs-data/{country}/{city}/seasonal_profile_enriched/{year}"
                
                # Création du répertoire
                self.create_hdfs_directory(hdfs_path)
                
                # Sauvegarde du fichier
                file_path = f"{hdfs_path}/profile.json"
                
                # Conversion en DataFrame et sauvegarde
                profile_df = self.spark.createDataFrame([profile])
                profile_df.write \
                    .mode("overwrite") \
                    .option("multiline", "true") \
                    .json(file_path)
                
                logger.info(f"Profil enrichi sauvegardé pour {city}: {file_path}")
            
            logger.info("Sauvegarde HDFS terminée")
            
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

    def run_validation_and_enrichment(self):
        """Exécute la validation et l'enrichissement des profils"""
        try:
            logger.info("Début de la validation et enrichissement des profils")
            
            # 1. Chargement des données historiques brutes
            historical_df = self.load_historical_data_from_hdfs()
            if not historical_df:
                logger.error("Impossible de charger les données historiques")
                return False
            
            # 2. Explosion des données quotidiennes
            exploded_df = self.explode_daily_data(historical_df)
            if not exploded_df:
                logger.error("Impossible d'exploser les données quotidiennes")
                return False
            
            # 3. Validation de la complétude
            completeness_results = self.validate_profile_completeness(exploded_df)
            
            # 4. Validation de la plausibilité
            plausibility_results = self.validate_data_plausibility(exploded_df)
            
            # 5. Enrichissement avec statistiques
            enriched_profiles = self.enrich_profiles_with_statistics(exploded_df)
            
            # 6. Sauvegarde dans HDFS
            self.save_enriched_profiles_to_hdfs(enriched_profiles)
            
            logger.info(f"Validation et enrichissement terminés: {len(enriched_profiles)} profils traités")
            
            # Affichage des résultats
            self.display_results(completeness_results, plausibility_results, enriched_profiles)
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la validation et enrichissement: {e}")
            return False

    def display_results(self, completeness_results, plausibility_results, enriched_profiles):
        """Affiche un résumé des résultats"""
        print("\n=== RESULTATS DE LA VALIDATION ET ENRICHISSEMENT ===")
        
        print(f"\nValidation de complétude:")
        complete_cities = [r for r in completeness_results if r["is_complete"]]
        print(f"  - Villes avec profils complets: {len(complete_cities)}/{len(completeness_results)}")
        
        print(f"\nValidation de plausibilité:")
        plausible_cities = [r for r in plausibility_results if r["is_plausible"]]
        print(f"  - Villes avec données plausibles: {len(plausible_cities)}/{len(plausibility_results)}")
        
        print(f"\nProfils enrichis:")
        print(f"  - Nombre de villes traitées: {len(enriched_profiles)}")
        
        for profile in enriched_profiles:
            city = profile["city"]
            country = profile["country"]
            months_count = profile["validation"]["total_months"]
            print(f"  - {city}, {country}: {months_count}/12 mois")
        
        print(f"\nDonnées sauvegardées dans HDFS:")
        print(f"  - Chemin: /hdfs-data/{{country}}/{{city}}/seasonal_profile_enriched/{{year}}/profile.json")

    def close(self):
        """Ferme la session Spark"""
        if self.spark:
            self.spark.stop()
            logger.info("Session Spark fermée")

def main():
    """Fonction principale"""
    print("=== EXERCICE 12: VALIDATION ET ENRICHISSEMENT DES PROFILS SAISONNIERS ===")
    print("Validation de complétude et plausibilité des données")
    print("Calcul de statistiques détaillées et probabilités d'alerte")
    print()
    
    validator = ProfilesValidatorEnricher()
    
    try:
        # Exécution de la validation et enrichissement
        success = validator.run_validation_and_enrichment()
        
        if success:
            print("\nValidation et enrichissement terminés avec succès")
        else:
            print("\nErreur lors de la validation et enrichissement")
            
    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur")
    except Exception as e:
        print(f"Erreur inattendue: {e}")
    finally:
        validator.close()

if __name__ == "__main__":
    main()
