#!/usr/bin/env python3
"""
Exercice 11 : Climatologie urbaine (profils saisonniers)
- Avec Spark, regrouper les données historiques par mois pour chaque ville
- Température moyenne par mois (profil saisonnier)
- Moyenne de vitesse du vent
- Probabilité d'alerte (level_1 ou level_2)
- Stockage dans HDFS : /hdfs-data/{country}/{city}/seasonal_profile/
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

class SeasonalProfilesAnalyzer:
    def __init__(self):
        # Configuration Spark
        self.spark = SparkSession.builder \
            .appName("SeasonalProfilesAnalyzer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Configuration HDFS
        self.hdfs_base_path = "/hdfs-data"
        self.hdfs_output_path = "/hdfs-data/{country}/{city}/seasonal_profile"
        
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
            
            # Ajout du mois extrait de la date
            exploded_df = exploded_df.withColumn("month", month(to_date(col("date"), "yyyy-MM-dd")))
            
            logger.info(f"Données explosées: {exploded_df.count()} enregistrements")
            return exploded_df
            
        except Exception as e:
            logger.error(f"Erreur lors de l'explosion des données: {e}")
            return None

    def calculate_seasonal_profiles(self, df):
        """Calcule les profils saisonniers par ville et par mois"""
        try:
            logger.info("Calcul des profils saisonniers...")
            
            # Calcul des moyennes par mois et par ville
            seasonal_profiles = df.groupBy("city", "country", "month") \
                .agg(
                    count("*").alias("days_count"),
                    avg("temp_max").alias("temp_mean"),
                    avg("windspeed_max").alias("wind_mean"),
                    avg("precipitation").alias("precip_mean")
                ) \
                .orderBy("city", "country", "month")
            
            # Calcul des probabilités d'alerte basées sur les codes météo
            alert_profiles = self.calculate_alert_probabilities(df)
            
            # Jointure des profils
            final_profiles = seasonal_profiles.join(
                alert_profiles,
                ["city", "country", "month"],
                "left"
            ).fillna(0.0, subset=["alert_probability_level_1", "alert_probability_level_2"])
            
            logger.info(f"Profils saisonniers calculés: {final_profiles.count()} enregistrements")
            return final_profiles
            
        except Exception as e:
            logger.error(f"Erreur lors du calcul des profils saisonniers: {e}")
            return None

    def calculate_alert_probabilities(self, df):
        """Calcule les probabilités d'alerte basées sur les codes météo"""
        try:
            # Codes météo Open-Meteo pour les alertes
            # Level 1: Pluie légère, neige légère, brouillard
            level_1_codes = [61, 63, 65, 71, 73, 75, 77, 80, 81, 82, 45, 48]
            # Level 2: Pluie forte, neige forte, orages, tempêtes
            level_2_codes = [66, 67, 85, 86, 95, 96, 99]
            
            # Fonction pour calculer les probabilités
            def calculate_alert_prob(weather_codes):
                if not weather_codes:
                    return (0.0, 0.0)
                
                total_days = len(weather_codes)
                level_1_count = sum(1 for code in weather_codes if code in level_1_codes)
                level_2_count = sum(1 for code in weather_codes if code in level_2_codes)
                
                prob_level_1 = level_1_count / total_days if total_days > 0 else 0.0
                prob_level_2 = level_2_count / total_days if total_days > 0 else 0.0
                
                return (prob_level_1, prob_level_2)
            
            # Collecte des codes météo par groupe
            weather_codes_by_group = df.groupBy("city", "country", "month") \
                .agg(collect_list("weathercode").alias("weather_codes")) \
                .collect()
            
            # Calcul des probabilités pour chaque groupe
            alert_data = []
            for row in weather_codes_by_group:
                city = row["city"]
                country = row["country"]
                month = row["month"]
                weather_codes = row["weather_codes"]
                
                prob_level_1, prob_level_2 = calculate_alert_prob(weather_codes)
                
                alert_data.append({
                    "city": city,
                    "country": country,
                    "month": month,
                    "alert_probability_level_1": prob_level_1,
                    "alert_probability_level_2": prob_level_2
                })
            
            # Création du DataFrame
            alert_df = self.spark.createDataFrame(alert_data)
            
            logger.info(f"Probabilités d'alerte calculées pour {len(alert_data)} groupes")
            return alert_df
            
        except Exception as e:
            logger.error(f"Erreur lors du calcul des probabilités d'alerte: {e}")
            return None

    def save_seasonal_profiles_to_hdfs(self, profiles_df):
        """Sauvegarde les profils saisonniers dans HDFS"""
        try:
            logger.info("Sauvegarde des profils saisonniers dans HDFS...")
            
            # Groupement par ville
            profiles_by_city = profiles_df.collect()
            cities = {}
            
            for profile in profiles_by_city:
                city = profile["city"]
                country = profile["country"]
                key = f"{country}_{city}"
                
                if key not in cities:
                    cities[key] = {
                        "city": city,
                        "country": country,
                        "profile_year": datetime.now().year,
                        "created_at": datetime.now().isoformat(),
                        "monthly_profiles": []
                    }
                
                # Ajout du profil mensuel
                monthly_profile = {
                    "month": profile["month"],
                    "days_count": profile["days_count"],
                    "temp_mean": round(float(profile["temp_mean"]), 2),
                    "wind_mean": round(float(profile["wind_mean"]), 2),
                    "precip_mean": round(float(profile["precip_mean"]), 2),
                    "alert_probability_level_1": round(float(profile.get("alert_probability_level_1", 0.0)), 3),
                    "alert_probability_level_2": round(float(profile.get("alert_probability_level_2", 0.0)), 3)
                }
                
                cities[key]["monthly_profiles"].append(monthly_profile)
            
            # Sauvegarde par ville
            for city_key, city_data in cities.items():
                city = city_data["city"]
                country = city_data["country"]
                
                # Construction du chemin HDFS
                hdfs_path = f"/hdfs-data/{country}/{city}/seasonal_profile"
                
                # Création du répertoire
                self.create_hdfs_directory(hdfs_path)
                
                # Sauvegarde du fichier
                filename = f"seasonal_profile_{city_data['profile_year']}.json"
                file_path = f"{hdfs_path}/{filename}"
                
                # Conversion en DataFrame et sauvegarde
                profile_df = self.spark.createDataFrame([city_data])
                profile_df.write \
                    .mode("overwrite") \
                    .option("multiline", "true") \
                    .json(file_path)
                
                logger.info(f"Profil saisonnier sauvegardé pour {city}: {file_path}")
            
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

    def run_analysis(self):
        """Exécute l'analyse complète des profils saisonniers"""
        try:
            logger.info("Début de l'analyse des profils saisonniers")
            
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
            
            # 3. Calcul des profils saisonniers
            profiles_df = self.calculate_seasonal_profiles(exploded_df)
            if not profiles_df:
                logger.error("Impossible de calculer les profils saisonniers")
                return False
            
            # 4. Sauvegarde dans HDFS
            self.save_seasonal_profiles_to_hdfs(profiles_df)
            
            logger.info("Analyse des profils saisonniers terminée avec succès")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de l'analyse: {e}")
            return False
        finally:
            # Affichage des résultats
            self.display_results(profiles_df)

    def display_results(self, profiles_df):
        """Affiche un résumé des résultats"""
        if not profiles_df:
            return
        
        print("\n=== RESULTATS DES PROFILS SAISONNIERS ===")
        
        # Groupement par ville
        profiles = profiles_df.collect()
        cities = {}
        for profile in profiles:
            city_key = f"{profile['city']}, {profile['country']}"
            if city_key not in cities:
                cities[city_key] = []
            cities[city_key].append(profile)
        
        for city_key, city_profiles in cities.items():
            print(f"\n{city_key}:")
            # Tri par mois
            city_profiles.sort(key=lambda x: x["month"])
            for profile in city_profiles:
                month_names = ["Jan", "Fév", "Mar", "Avr", "Mai", "Juin", 
                             "Juil", "Août", "Sep", "Oct", "Nov", "Déc"]
                month_name = month_names[profile["month"] - 1]
                print(f"  - {month_name}: Temp {profile['temp_mean']:.1f}°C, "
                      f"Vent {profile['wind_mean']:.1f} km/h, "
                      f"Alertes L1: {profile.get('alert_probability_level_1', 0):.1%}, "
                      f"L2: {profile.get('alert_probability_level_2', 0):.1%}")
        
        print(f"\nTotal: {len(profiles)} profils mensuels créés")
        print("Données sauvegardées dans HDFS:")
        print("  - Chemin: /hdfs-data/{country}/{city}/seasonal_profile/seasonal_profile_{year}.json")

    def close(self):
        """Ferme la session Spark"""
        if self.spark:
            self.spark.stop()
            logger.info("Session Spark fermée")

def main():
    """Fonction principale"""
    print("=== EXERCICE 11: CLIMATOLOGIE URBAINE (PROFILS SAISONNIERS) ===")
    print("Regroupement des données historiques par mois pour créer des profils saisonniers")
    print("Calcul des moyennes mensuelles et probabilités d'alerte")
    print()
    
    analyzer = SeasonalProfilesAnalyzer()
    
    try:
        # Exécution de l'analyse
        success = analyzer.run_analysis()
        
        if success:
            print("\nAnalyse des profils saisonniers terminée avec succès")
        else:
            print("\nErreur lors de l'analyse des profils saisonniers")
            
    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur")
    except Exception as e:
        print(f"Erreur inattendue: {e}")
    finally:
        analyzer.close()

if __name__ == "__main__":
    main()
