#!/usr/bin/env python3
"""
Exercice 8 : Visualisation et agregation des logs meteo
Consomme les logs HDFS et implemente les visualisations demandees
"""

import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
from collections import Counter
import sys

class WeatherVisualizer:
    def __init__(self, hdfs_path="hdfs-data"):
        """Initialise le visualisateur avec le chemin HDFS"""
        self.hdfs_path = Path(hdfs_path)
        self.weather_data = []
        self.df = None
        
        # Configuration des graphiques
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
    def load_hdfs_data(self):
        """Charge toutes les donnees depuis la structure HDFS"""
        print("=== CHARGEMENT DONNEES HDFS ===")
        
        if not self.hdfs_path.exists():
            print(f"Repertoire HDFS non trouve: {self.hdfs_path}")
            return False
        
        total_files = 0
        total_alerts = 0
        
        # Parcours de la structure HDFS
        for country_dir in self.hdfs_path.iterdir():
            if country_dir.is_dir():
                print(f"Chargement pays: {country_dir.name}")
                
                for city_dir in country_dir.iterdir():
                    if city_dir.is_dir():
                        alerts_file = city_dir / "alerts.json"
                        if alerts_file.exists():
                            try:
                                with open(alerts_file, 'r', encoding='utf-8') as f:
                                    alerts = json.load(f)
                                    
                                for alert in alerts:
                                    # Enrichissement avec pays/ville
                                    alert['country'] = country_dir.name
                                    alert['city'] = city_dir.name
                                    self.weather_data.append(alert)
                                
                                total_files += 1
                                total_alerts += len(alerts)
                                print(f"  {city_dir.name}: {len(alerts)} alertes")
                                
                            except Exception as e:
                                print(f"  Erreur lecture {alerts_file}: {e}")
        
        print(f"\nTotal charge:")
        print(f"  Fichiers: {total_files}")
        print(f"  Alertes: {total_alerts}")
        
        if self.weather_data:
            # Conversion en DataFrame pandas
            self.df = pd.DataFrame(self.weather_data)
            
            # Conversion des timestamps
            self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
            self.df['event_time'] = pd.to_datetime(self.df['event_time'])
            
            # Extraction des donnees meteo (gestion des cles manquantes)
            weather_df = pd.json_normalize(self.df['weather_data'].fillna({}))
            alerts_df = pd.json_normalize(self.df['alerts'].fillna({}))
            location_df = pd.json_normalize(self.df['location'].fillna({}))
            
            # Fusion des donnees
            self.df = pd.concat([
                self.df[['timestamp', 'event_time', 'country', 'city']],
                weather_df,
                alerts_df,
                location_df
            ], axis=1)
            
            print(f"DataFrame cree: {len(self.df)} lignes, {len(self.df.columns)} colonnes")
            return True
        
        return False
    
    def plot_temperature_evolution(self):
        """Graphique evolution de la temperature au fil du temps"""
        if self.df is None or self.df.empty:
            print("Aucune donnee pour la visualisation temperature")
            return
        
        plt.figure(figsize=(12, 6))
        
        # Groupement par ville pour les courbes
        cities = self.df['city'].value_counts().head(5).index
        
        for city in cities:
            city_data = self.df[self.df['city'] == city].sort_values('timestamp')
            if len(city_data) > 1:
                plt.plot(city_data['timestamp'], city_data['temperature'], 
                        marker='o', label=f"{city}", linewidth=2)
        
        plt.title('Evolution de la Temperature au Fil du Temps', fontsize=16, fontweight='bold')
        plt.xlabel('Temps', fontsize=12)
        plt.ylabel('Temperature (°C)', fontsize=12)
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Sauvegarde
        plt.savefig('temperature_evolution.png', dpi=300, bbox_inches='tight')
        print("Graphique sauvegarde: temperature_evolution.png")
        plt.show()
    
    def plot_wind_evolution(self):
        """Graphique evolution de la vitesse du vent"""
        if self.df is None or self.df.empty:
            print("Aucune donnee pour la visualisation vent")
            return
        
        plt.figure(figsize=(12, 6))
        
        # Groupement par ville
        cities = self.df['city'].value_counts().head(5).index
        
        for city in cities:
            city_data = self.df[self.df['city'] == city].sort_values('timestamp')
            if len(city_data) > 1:
                plt.plot(city_data['timestamp'], city_data['windspeed'], 
                        marker='s', label=f"{city}", linewidth=2)
        
        plt.title('Evolution de la Vitesse du Vent au Fil du Temps', fontsize=16, fontweight='bold')
        plt.xlabel('Temps', fontsize=12)
        plt.ylabel('Vitesse du Vent (m/s)', fontsize=12)
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Sauvegarde
        plt.savefig('wind_evolution.png', dpi=300, bbox_inches='tight')
        print("Graphique sauvegarde: wind_evolution.png")
        plt.show()
    
    def plot_alerts_by_level(self):
        """Graphique nombre d'alertes vent et chaleur par niveau"""
        if self.df is None or self.df.empty:
            print("Aucune donnee pour la visualisation alertes")
            return
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Alertes vent
        wind_counts = self.df['wind_alert_level'].value_counts()
        colors_wind = ['green', 'orange', 'red']
        ax1.bar(wind_counts.index, wind_counts.values, color=colors_wind[:len(wind_counts)])
        ax1.set_title('Nombre d\'Alertes Vent par Niveau', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Niveau d\'Alerte Vent', fontsize=12)
        ax1.set_ylabel('Nombre d\'Alertes', fontsize=12)
        
        # Ajout des valeurs sur les barres
        for i, v in enumerate(wind_counts.values):
            ax1.text(i, v + 0.5, str(v), ha='center', va='bottom', fontweight='bold')
        
        # Alertes chaleur
        heat_counts = self.df['heat_alert_level'].value_counts()
        colors_heat = ['lightblue', 'orange', 'red']
        ax2.bar(heat_counts.index, heat_counts.values, color=colors_heat[:len(heat_counts)])
        ax2.set_title('Nombre d\'Alertes Chaleur par Niveau', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Niveau d\'Alerte Chaleur', fontsize=12)
        ax2.set_ylabel('Nombre d\'Alertes', fontsize=12)
        
        # Ajout des valeurs sur les barres
        for i, v in enumerate(heat_counts.values):
            ax2.text(i, v + 0.5, str(v), ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        
        # Sauvegarde
        plt.savefig('alerts_by_level.png', dpi=300, bbox_inches='tight')
        print("Graphique sauvegarde: alerts_by_level.png")
        plt.show()
    
    def plot_weather_codes_by_country(self):
        """Graphique code meteo le plus frequent par pays"""
        if self.df is None or self.df.empty:
            print("Aucune donnee pour la visualisation codes meteo")
            return
        
        # Codes meteo et leurs descriptions
        weather_code_desc = {
            0: 'Ciel clair',
            1: 'Principalement clair',
            2: 'Partiellement nuageux',
            3: 'Couvert',
            45: 'Brouillard',
            48: 'Brouillard givrant',
            51: 'Bruine legere',
            61: 'Pluie legere',
            80: 'Averses'
        }
        
        # Groupement par pays
        countries = self.df['country'].value_counts().head(6).index
        
        fig, axes = plt.subplots(2, 3, figsize=(18, 10))
        axes = axes.flatten()
        
        for i, country in enumerate(countries):
            if i >= 6:
                break
                
            country_data = self.df[self.df['country'] == country]
            weather_codes = country_data['weathercode'].value_counts()
            
            # Graphique en secteurs
            labels = [f"Code {code}\n{weather_code_desc.get(code, 'Inconnu')}" 
                     for code in weather_codes.index]
            
            axes[i].pie(weather_codes.values, labels=labels, autopct='%1.1f%%', startangle=90)
            axes[i].set_title(f'Codes Meteo - {country}', fontsize=12, fontweight='bold')
        
        # Masquer les axes non utilises
        for j in range(len(countries), 6):
            axes[j].axis('off')
        
        plt.suptitle('Code Meteo le Plus Frequent par Pays', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        # Sauvegarde
        plt.savefig('weather_codes_by_country.png', dpi=300, bbox_inches='tight')
        print("Graphique sauvegarde: weather_codes_by_country.png")
        plt.show()
    
    def generate_summary_report(self):
        """Genere un rapport de synthese"""
        if self.df is None or self.df.empty:
            print("Aucune donnee pour le rapport")
            return
        
        print("\n=== RAPPORT DE SYNTHESE ===")
        
        # Statistiques generales
        print(f"Periode d'analyse: {self.df['timestamp'].min()} a {self.df['timestamp'].max()}")
        print(f"Nombre total d'observations: {len(self.df)}")
        print(f"Nombre de pays: {self.df['country'].nunique()}")
        print(f"Nombre de villes: {self.df['city'].nunique()}")
        
        # Temperature
        print(f"\nStatistiques temperature:")
        print(f"  Moyenne: {self.df['temperature'].mean():.1f}°C")
        print(f"  Minimum: {self.df['temperature'].min():.1f}°C")
        print(f"  Maximum: {self.df['temperature'].max():.1f}°C")
        
        # Vent
        print(f"\nStatistiques vent:")
        print(f"  Vitesse moyenne: {self.df['windspeed'].mean():.1f} m/s")
        print(f"  Vitesse minimum: {self.df['windspeed'].min():.1f} m/s")
        print(f"  Vitesse maximum: {self.df['windspeed'].max():.1f} m/s")
        
        # Alertes
        print(f"\nRepartition des alertes:")
        print(f"  Alertes vent:")
        wind_counts = self.df['wind_alert_level'].value_counts()
        for level, count in wind_counts.items():
            print(f"    {level}: {count}")
        
        print(f"  Alertes chaleur:")
        heat_counts = self.df['heat_alert_level'].value_counts()
        for level, count in heat_counts.items():
            print(f"    {level}: {count}")
        
        # Top villes
        print(f"\nTop 5 villes par nombre d'observations:")
        city_counts = self.df['city'].value_counts().head(5)
        for city, count in city_counts.items():
            print(f"  {city}: {count} observations")
        
        # Codes meteo
        print(f"\nCodes meteo les plus frequents:")
        weather_codes = self.df['weathercode'].value_counts().head(3)
        weather_code_desc = {
            0: 'Ciel clair', 1: 'Principalement clair', 2: 'Partiellement nuageux',
            3: 'Couvert', 45: 'Brouillard', 61: 'Pluie legere'
        }
        for code, count in weather_codes.items():
            desc = weather_code_desc.get(code, 'Inconnu')
            print(f"  Code {code} ({desc}): {count} observations")

def main():
    """Fonction principale"""
    print("=== VISUALISATION LOGS METEO - EXERCICE 8 ===")
    print("Analyse et visualisation des donnees HDFS")
    print("-" * 60)
    
    # Verification du repertoire HDFS
    hdfs_path = Path("../exo7/hdfs-data")
    if not hdfs_path.exists():
        print(f"Repertoire HDFS non trouve: {hdfs_path}")
        print("Lancez d'abord l'exercice 7 pour generer les donnees HDFS")
        return 1
    
    visualizer = WeatherVisualizer(hdfs_path)
    
    try:
        # Chargement des donnees
        if not visualizer.load_hdfs_data():
            print("Echec du chargement des donnees HDFS")
            return 1
        
        # Generation du rapport de synthese
        visualizer.generate_summary_report()
        
        print(f"\n=== GENERATION DES VISUALISATIONS ===")
        
        # Visualisation 1: Evolution temperature
        print("1. Evolution de la temperature au fil du temps...")
        visualizer.plot_temperature_evolution()
        
        # Visualisation 2: Evolution vent
        print("2. Evolution de la vitesse du vent...")
        visualizer.plot_wind_evolution()
        
        # Visualisation 3: Alertes par niveau
        print("3. Nombre d'alertes vent et chaleur par niveau...")
        visualizer.plot_alerts_by_level()
        
        # Visualisation 4: Codes meteo par pays
        print("4. Code meteo le plus frequent par pays...")
        visualizer.plot_weather_codes_by_country()
        
        print(f"\n=== VISUALISATIONS TERMINEES ===")
        print("Fichiers generes:")
        print("  - temperature_evolution.png")
        print("  - wind_evolution.png")
        print("  - alerts_by_level.png")
        print("  - weather_codes_by_country.png")
        
    except Exception as e:
        print(f"Erreur: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
