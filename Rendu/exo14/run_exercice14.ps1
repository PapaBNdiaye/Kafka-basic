# Script d'exécution pour l'exercice 14
# Frontend global de visualisation météo

Write-Host "=== EXERCICE 14: FRONTEND GLOBAL DE VISUALISATION METEO ===" -ForegroundColor Green
Write-Host "Interface web Flask regroupant tous les dashboards et visualisations" -ForegroundColor Cyan

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

# Vérification des topics Kafka
Write-Host "Topics Kafka disponibles:" -ForegroundColor Cyan
& "..\..\..\kafka\bin\windows\kafka-topics.bat" --list --bootstrap-server localhost:9092

# Vérification des données HDFS
Write-Host "`nDonnées HDFS disponibles:" -ForegroundColor Cyan
hdfs dfs -ls /hdfs-data/

Write-Host "`nTous les prérequis sont satisfaits. Lancement de l'application Flask..." -ForegroundColor Green

Write-Host "`n[INFO] L'application Flask va démarrer sur http://localhost:5000" -ForegroundColor Yellow
Write-Host "Appuyez sur Ctrl+C pour arrêter l'application." -ForegroundColor Yellow

# Exécution de l'application Flask
try {
    python weather_dashboard_app.py
    Write-Host "`n=== APPLICATION TERMINEE ===" -ForegroundColor Green
} catch {
    Write-Host "`n[ERREUR] Erreur lors de l'exécution de l'application Flask: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Write-Host "`n=== EXERCICE 14 TERMINE ===" -ForegroundColor Green
