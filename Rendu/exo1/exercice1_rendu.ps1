# Exercice 1 - Script de rendu
# Mise en place de Kafka et producteur simple

Write-Host "=== EXERCICE 1 TP KAFKA ===" -ForegroundColor Green

# Navigation vers Kafka
cd "C:\kafka\kafka_2.13-3.9.1"

Write-Host "`nEtape 1: Creation du topic weather_stream" -ForegroundColor Yellow
bin\windows\kafka-topics.bat --create --topic weather_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

Write-Host "`nEtape 2: Verification du topic" -ForegroundColor Yellow  
bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

Write-Host "`nEtape 3: Envoi du message JSON" -ForegroundColor Yellow
$message = '{"msg": "Hello Kafka"}'
echo $message | bin\windows\kafka-console-producer.bat --topic weather_stream --bootstrap-server localhost:9092

Write-Host "`nEtape 4: Verification avec consommateur (3 secondes)" -ForegroundColor Yellow
bin\windows\kafka-console-consumer.bat --topic weather_stream --from-beginning --bootstrap-server localhost:9092 --timeout-ms 3000

Write-Host "`n=== EXERCICE 1 TERMINE ===" -ForegroundColor Green
