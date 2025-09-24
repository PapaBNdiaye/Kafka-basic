# Script d'exécution pour l'exercice 9
# Récupération de séries historiques longues

Write-Host "=== EXERCICE 9: RECUPERATION DE SERIES HISTORIQUES LONGUES ===" -ForegroundColor Green
Write-Host "Activation de l'environnement virtuel et exécution du script Python" -ForegroundColor Cyan

# Activation de l'environnement virtuel
if (Test-Path "..\..\venv\Scripts\Activate.ps1") {
    Write-Host "Activation de l'environnement virtuel..." -ForegroundColor Yellow
    & "..\..\venv\Scripts\Activate.ps1"
} else {
    Write-Host "Environnement virtuel non trouvé. Vérifiez le chemin." -ForegroundColor Red
    exit 1
}

# Configuration des variables d'environnement Hadoop
$env:JAVA_HOME = "C:\Program Files\Java\jdk1.8.0_XXX"
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:HADOOP_HOME\sbin;$env:PATH"

Write-Host "Variables d'environnement configurées:" -ForegroundColor Yellow
Write-Host "JAVA_HOME: $env:JAVA_HOME" -ForegroundColor Gray
Write-Host "HADOOP_HOME: $env:HADOOP_HOME" -ForegroundColor Gray

# Vérification des services requis
Write-Host "`nVérification des services requis..." -ForegroundColor Yellow

$services = @(
    @{Name="ZooKeeper"; Port=2181},
    @{Name="Kafka"; Port=9092},
    @{Name="HDFS NameNode"; Port=9870},
    @{Name="HDFS DataNode"; Port=9864}
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

Write-Host "`nTous les services requis sont actifs. Lancement du script Python..." -ForegroundColor Green

# Exécution du script Python
try {
    python weather_history_retriever.py
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
Write-Host "`nDonnées HDFS créées:" -ForegroundColor Cyan
hdfs dfs -ls /hdfs-data/France/Paris/weather_history_raw/

Write-Host "`n=== EXERCICE 9 TERMINE ===" -ForegroundColor Green
