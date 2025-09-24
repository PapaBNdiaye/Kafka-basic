# Script d'exécution pour l'exercice 13
# Détection d'anomalies climatiques (Batch vs Speed)

Write-Host "=== EXERCICE 13: DETECTION D'ANOMALIES CLIMATIQUES (BATCH VS SPEED) ===" -ForegroundColor Green
Write-Host "Spark Streaming pour détecter les anomalies en temps réel" -ForegroundColor Cyan

# Activation de l'environnement virtuel
if (Test-Path "..\..\venv\Scripts\Activate.ps1") {
    Write-Host "Activation de l'environnement virtuel..." -ForegroundColor Yellow
    & "..\..\venv\Scripts\Activate.ps1"
} else {
    Write-Host "Environnement virtuel non trouvé. Vérifiez le chemin." -ForegroundColor Red
    exit 1
}

# Configuration des variables d'environnement
$env:JAVA_HOME = "C:\Program Files\Java\jdk1.8.0_XXX"
$env:HADOOP_HOME = "C:\hadoop"
$env:SPARK_HOME = "C:\spark"
$env:PATH = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:HADOOP_HOME\sbin;$env:SPARK_HOME\bin;$env:PATH"

Write-Host "Variables d'environnement configurées:" -ForegroundColor Yellow
Write-Host "JAVA_HOME: $env:JAVA_HOME" -ForegroundColor Gray
Write-Host "HADOOP_HOME: $env:HADOOP_HOME" -ForegroundColor Gray
Write-Host "SPARK_HOME: $env:SPARK_HOME" -ForegroundColor Gray

# Vérification des services requis
Write-Host "`nVérification des services requis..." -ForegroundColor Yellow

$services = @(
    @{Name="ZooKeeper"; Port=2181},
    @{Name="Kafka"; Port=9092},
    @{Name="HDFS NameNode"; Port=9870},
    @{Name="HDFS DataNode"; Port=9864},
    @{Name="YARN ResourceManager"; Port=8088},
    @{Name="YARN NodeManager"; Port=8042}
)

$allServicesRunning = $true
foreach ($service in $services) {
    $connection = Test-NetConnection -ComputerName localhost -Port $service.Port -InformationLevel Quiet -WarningAction SilentlyContinue
    if ($connection) {
        Write-Host "[OK] $($service.Name) (port $($service.Port)) : ACTIF" -ForegroundColor Green
    } else {
        Write-Host "[KO] $($service.Name) (port $($service.Port)) : INACTIF" -ForegroundColor Red
        $allServicesRunning = $false
    }
}

if (-not $allServicesRunning) {
    Write-Host "`n[ERREUR] Certains services requis ne sont pas actifs." -ForegroundColor Red
    Write-Host "Veuillez démarrer les services manquants avant de continuer." -ForegroundColor Red
    exit 1
}

# Vérification des prérequis
Write-Host "`nVérification des prérequis..." -ForegroundColor Yellow

# Vérification du topic weather_transformed
$topicExists = $false
try {
    $result = & "..\..\..\kafka\bin\windows\kafka-topics.bat" --list --bootstrap-server localhost:9092 | Select-String "weather_transformed"
    if ($result) {
        Write-Host "[OK] Topic weather_transformed trouvé" -ForegroundColor Green
        $topicExists = $true
    } else {
        Write-Host "[KO] Topic weather_transformed non trouvé" -ForegroundColor Red
    }
} catch {
    Write-Host "[KO] Erreur lors de la vérification des topics Kafka" -ForegroundColor Red
}

if (-not $topicExists) {
    Write-Host "`n[INFO] Création du topic weather_transformed..." -ForegroundColor Yellow
    try {
        & "..\..\..\kafka\bin\windows\kafka-topics.bat" --create --topic weather_transformed --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
        Write-Host "[OK] Topic weather_transformed créé" -ForegroundColor Green
    } catch {
        Write-Host "[KO] Erreur lors de la création du topic" -ForegroundColor Red
        exit 1
    }
}

# Vérification des profils enrichis de l'exercice 12
$profilesExist = $false
try {
    $result = hdfs dfs -ls /hdfs-data/France/Paris/seasonal_profile_enriched/ 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] Profils enrichis de l'exercice 12 trouvés dans HDFS" -ForegroundColor Green
        $profilesExist = $true
    } else {
        Write-Host "[KO] Profils enrichis de l'exercice 12 non trouvés dans HDFS" -ForegroundColor Red
    }
} catch {
    Write-Host "[KO] Erreur lors de la vérification des profils enrichis" -ForegroundColor Red
}

if (-not $profilesExist) {
    Write-Host "`n[ERREUR] L'exercice 12 doit être exécuté en premier pour générer les profils enrichis." -ForegroundColor Red
    exit 1
}

Write-Host "`nTous les prérequis sont satisfaits. Lancement du script Python..." -ForegroundColor Green

Write-Host "`n[INFO] Le script va démarrer en mode streaming et attendre les données du topic weather_transformed." -ForegroundColor Yellow
Write-Host "Appuyez sur Ctrl+C pour arrêter le streaming." -ForegroundColor Yellow

# Exécution du script Python
try {
    python anomaly_detector.py
    Write-Host "`n=== EXECUTION TERMINEE ===" -ForegroundColor Green
} catch {
    Write-Host "`n[ERREUR] Erreur lors de l'exécution du script Python: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Write-Host "`nVérification des résultats..." -ForegroundColor Yellow

# Vérification des topics Kafka
Write-Host "Topics Kafka créés:" -ForegroundColor Cyan
& "..\..\..\kafka\bin\windows\kafka-topics.bat" --list --bootstrap-server localhost:9092

# Vérification des données HDFS
Write-Host "`nAnomalies détectées dans HDFS:" -ForegroundColor Cyan
hdfs dfs -ls /hdfs-data/France/Paris/anomalies/

Write-Host "`n=== EXERCICE 13 TERMINE ===" -ForegroundColor Green
