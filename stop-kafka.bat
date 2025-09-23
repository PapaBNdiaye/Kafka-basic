@echo off
REM Script pour arreter Kafka et ZooKeeper
echo Arret de Kafka et ZooKeeper
cd /d "%~dp0kafka_2.13-3.9.1"
bin\windows\kafka-server-stop.bat
timeout /t 5 /nobreak >nul
bin\windows\zookeeper-server-stop.bat
echo Services arretes.
pause
