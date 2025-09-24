# Script d'exécution pour l'exercice 12
# Validation et enrichissement des profils saisonniers

Write-Host "=== EXERCICE 12: VALIDATION ET ENRICHISSEMENT DES PROFILS SAISONNIERS ===" -ForegroundColor Green
Write-Host "Validation, calculs statistiques et enrichissement des profils saisonniers" -ForegroundColor Cyan

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

# Vérification des données de l'exercice 9
Write-Host "`nVérification des données de l'exercice 9..." -ForegroundColor Yellow
$hdfsDataExists = $false
try {
    $result = hdfs dfs -ls /hdfs-data/France/Paris/weather_history_raw/ 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] Données de l'exercice 9 trouvées dans HDFS" -ForegroundColor Green
        $hdfsDataExists = $true
    } else {
        Write-Host "[KO] Données de l'exercice 9 non trouvées dans HDFS" -ForegroundColor Red
    }
} catch {
    Write-Host "[KO] Erreur lors de la vérification des données HDFS" -ForegroundColor Red
}

if (-not $hdfsDataExists) {
    Write-Host "`n[ERREUR] L'exercice 9 doit être exécuté en premier pour générer les données historiques." -ForegroundColor Red
    exit 1
}

Write-Host "`nTous les prérequis sont satisfaits. Lancement du script Python..." -ForegroundColor Green

# Exécution du script Python
try {
    python profiles_validator_enricher.py
    Write-Host "`n=== EXECUTION TERMINEE ===" -ForegroundColor Green
} catch {
    Write-Host "`n[ERREUR] Erreur lors de l'exécution du script Python: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Write-Host "`nVérification des résultats..." -ForegroundColor Yellow

# Vérification des données HDFS
Write-Host "Profils enrichis créés dans HDFS:" -ForegroundColor Cyan
hdfs dfs -ls /hdfs-data/France/Paris/seasonal_profile_enriched/

Write-Host "`n=== EXERCICE 12 TERMINE ===" -ForegroundColor Green
