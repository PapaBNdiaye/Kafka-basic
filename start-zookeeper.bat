@echo off
REM Script pour demarrer ZooKeeper (requis pour Kafka)
echo Demarrage de ZooKeeper
cd /d "C:\kafka\kafka_2.13-3.9.1"
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
pause