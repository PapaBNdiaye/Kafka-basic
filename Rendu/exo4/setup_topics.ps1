# Script de configuration pour l'exercice 4
# Création du topic weather_transformed

Write-Host "=== SETUP EXERCICE 4 ===" -ForegroundColor Green
Write-Host "Configuration des topics Kafka" -ForegroundColor Cyan

cd "C:\kafka\kafka_2.13-3.9.1"

Write-Host "`nCreation du topic weather_transformed..." -ForegroundColor Yellow
bin\windows\kafka-topics.bat --create --topic weather_transformed --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

Write-Host "`nVerification des topics crees..." -ForegroundColor Yellow
bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

Write-Host "`nDescription du topic weather_transformed..." -ForegroundColor Yellow
bin\windows\kafka-topics.bat --describe --topic weather_transformed --bootstrap-server localhost:9092

Write-Host "`n=== SETUP TERMINE ===" -ForegroundColor Green
Write-Host "Topic weather_transformed pret pour Spark Streaming" -ForegroundColor Green
