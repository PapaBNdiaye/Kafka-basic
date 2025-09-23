#!/usr/bin/env python3
"""
Exercice 2 : Consommateur Kafka
Script Python qui lit les messages depuis un topic Kafka passe en argument
et affiche les messages recus en temps reel
"""

import sys
import json
from kafka import KafkaConsumer
from datetime import datetime

def main():
    # Verification des arguments
    if len(sys.argv) != 2:
        print("Usage: python consumer.py <nom_du_topic>")
        print("Exemple: python consumer.py weather_stream")
        sys.exit(1)
    
    topic_name = sys.argv[1]
    
    print(f"=== CONSOMMATEUR KAFKA - EXERCICE 2 ===")
    print(f"Topic: {topic_name}")
    print(f"Serveur: localhost:9092")
    print(f"Demarrage de la consommation en temps reel")
    print("-" * 50)
    
    try:
        # Creation du consommateur Kafka
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',  # Lit seulement les nouveaux messages
            enable_auto_commit=True,
            group_id='python-consumer-group',
            value_deserializer=lambda x: x.decode('utf-8') if x else None
        )
        
        print(f"[OK] Consommateur connecte au topic '{topic_name}'")
        print("En attente de messages (Ctrl+C pour arreter)")
        print()
        
        message_count = 0
        
        # Boucle de consommation en temps reel
        for message in consumer:
            message_count += 1
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            print(f"[{timestamp}] Message #{message_count}")
            print(f"  Partition: {message.partition}")
            print(f"  Offset: {message.offset}")
            
            # Tentative de parser le JSON si possible
            try:
                json_data = json.loads(message.value)
                print(f"  Contenu JSON: {json.dumps(json_data, indent=2, ensure_ascii=False)}")
            except (json.JSONDecodeError, TypeError):
                print(f"  Contenu texte: {message.value}")
            
            print("-" * 30)
            
    except KeyboardInterrupt:
        print(f"\n[STOP] Arret du consommateur. {message_count} messages traites.")
        
    except Exception as e:
        print(f"[ERR] Erreur: {e}")
        print("Verifiez que Kafka est demarre et que le topic existe.")
        sys.exit(1)
    
    finally:
        try:
            consumer.close()
            print("[OK] Consommateur ferme proprement.")
        except:
            pass

if __name__ == "__main__":
    main()
