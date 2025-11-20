# Complete Database Update Script
# Uploads to Cloud Storage AND updates production VM

param(
    [switch]$SkipScraping = $false,
    [string]$Date,
    [switch]$PreflightOnly = $false
)

$LOG_FILE = "e:\Dalal Street Trae\update_log.txt"
$DATE = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

function Write-Log {
    param($Message, $Color = "White")
    $LogMessage = "[$DATE] $Message"
    Add-Content -Path $LOG_FILE -Value $LogMessage
    Write-Host $Message -ForegroundColor $Color
}

Write-Log "========================================" "Cyan"
Write-Log "Database Update Started" "Cyan"
Write-Log "========================================" "Cyan"

$ErrorActionPreference = "Stop"

Write-Log "=================================================" "Cyan"
Write-Log "COMPLETE DATABASE UPDATE WORKFLOW" "Cyan"
Write-Log "=================================================" "Cyan"
Write-Log ""

function Run-PreflightCheck {
    Write-Log "[PREFLIGHT] Validating environment and prerequisites" "Cyan"
    Write-Log ""

    try {
        $gcloudVersion = & gcloud --version 2>$null
        if ($LASTEXITCODE -ne 0) { Write-Log "[ERROR] gcloud not found" "Red"; throw }
        Write-Log "[OK] gcloud available"

        $authList = & gcloud auth list
        if ($authList -notmatch "ACTIVE") { Write-Log "[WARN] No ACTIVE account in gcloud auth list" "Yellow" }
        $proj = & gcloud config get-value project
        if (-not $proj -or $proj -eq "") { Write-Log "[ERROR] gcloud project not set" "Red"; throw }
        Write-Log ("[OK] gcloud project: {0}" -f $proj)

        if (-not (Test-Path "$env:USERPROFILE\.ssh\google_compute_engine")) { Write-Log "[WARN] SSH private key missing: google_compute_engine" "Yellow" }
        if (-not (Test-Path "$env:USERPROFILE\.ssh\google_compute_engine.pub")) { Write-Log "[WARN] SSH public key missing: google_compute_engine.pub" "Yellow" }
        & gcloud compute ssh dalal-street-backend --zone=us-central1-a --command="echo ok" | Out-Null
        if ($LASTEXITCODE -eq 0) { Write-Log "[OK] SSH to VM works (non-interactive)" }
        else { Write-Log "[ERROR] SSH to VM failed (check keys or project/zone)" "Red" }

        $py = Get-Command python -ErrorAction SilentlyContinue
        if (-not $py) { Write-Log "[ERROR] python not found on PATH" "Red"; throw }
        else { Write-Log ("[OK] python: {0}" -f $py.Path) }

        $gsu = Get-Command gsutil -ErrorAction SilentlyContinue
        if (-not $gsu) { Write-Log "[ERROR] gsutil not found on PATH" "Red"; throw }
        else { Write-Log ("[OK] gsutil: {0}" -f $gsu.Path) }

        $dbOk = Test-Path "e:\Dalal Street Trae\App\database\stock_market_new.db"
        Write-Log ("[DB] stock_market_new.db present: {0}" -f $dbOk)
        $smOk = Test-Path "e:\Dalal Street Trae\App\database\stock_master.csv"
        Write-Log ("[CSV] stock_master.csv present: {0}" -f $smOk)
        $xlOk = Test-Path "e:\Dalal Street Trae\App\database\Company_Name_Changes_NSE.xlsx"
        Write-Log ("[Excel] Company_Name_Changes_NSE.xlsx present: {0}" -f $xlOk)
        $ipoFiles = Get-ChildItem -Path "e:\Dalal Street Trae\App\database" -Filter "IPO-PastIssue-*.csv" -ErrorAction SilentlyContinue
        Write-Log ("[CSV] IPO-PastIssue-*.csv files: {0}" -f ($ipoFiles.Count))
        $cfcaFiles = Get-ChildItem -Path "e:\Dalal Street Trae\App\database" -Filter "CF-CA*.csv" -ErrorAction SilentlyContinue
        Write-Log ("[CSV] CF-CA*.csv files: {0}" -f ($cfcaFiles.Count))

        & gcloud compute ssh dalal-street-backend --zone=us-central1-a --command="test -x ~/download_database.sh"
        if ($LASTEXITCODE -eq 0) { Write-Log "[OK] VM script ~/download_database.sh is executable" }
        else { Write-Log "[ERROR] VM script missing or not executable" "Red" }

        & gcloud compute ssh dalal-street-backend --zone=us-central1-a --command="docker ps --format '{{.Names}}' | grep -x dalal-backend"
        if ($LASTEXITCODE -eq 0) { Write-Log "[OK] Docker container 'dalal-backend' found" }
        else { Write-Log "[ERROR] Docker container 'dalal-backend' not found" "Red" }

        Write-Log ""
        Write-Log "[PREFLIGHT] Completed" "Green"
    } catch {
        Write-Log ("[PREFLIGHT] Failed: {0}" -f $_.Exception.Message) "Red"
        exit 1
    }
}

if ($PreflightOnly) {
    Run-PreflightCheck
    exit 0
}

# Step 1: Run scraping (optional)
if (-Not $SkipScraping) {
    Write-Log "[STEP 1/3] Running data scraping..." "Green"
    Write-Log ""
    
    cd "e:\Dalal Street Trae"
    $runDate = if ($Date) { $Date } else { Get-Date -Format "yyyy-MM-dd" }
    Write-Log ("Running authoritative runner for date: {0}" -f $runDate) "Yellow"
    python App\scriptsrebuild\AUTHORITATIVE_DAILY_RUNNER.py --date $runDate
    
    if ($LASTEXITCODE -eq 0) {
        Write-Log "✓ Scraping completed successfully" "Green"
        Write-Log ""
    } else {
        Write-Log "❌ Scraping failed" "Red"
        exit 1
    }
} else {
    Write-Log "[STEP 1/3] Skipping scraping (using existing database)" "Yellow"
    Write-Log ""
}

# Step 2: Upload to Cloud Storage
Write-Log "[STEP 2/3] Uploading to Cloud Storage..." "Green"
Write-Log ""

& "e:\Dalal Street Trae\upload_database_to_cloud.ps1"

if ($LASTEXITCODE -ne 0) {
    Write-Log "❌ Upload failed - aborting" "Red"
    exit 1
}

Write-Log ""

# Step 3: Update VM
Write-Log "[STEP 3/3] Updating production VM..." "Green"
Write-Log ""

# SSH into VM and run update script
gcloud compute ssh dalal-street-backend --zone=us-central1-a --command="~/download_database.sh"

if ($LASTEXITCODE -eq 0) {
    Write-Log ""
    Write-Log "=================================================" "Cyan"
    Write-Log "🎉 COMPLETE UPDATE SUCCESSFUL!" "Green"
    Write-Log "=================================================" "Cyan"
    Write-Log ""
    Write-Log "Your production backend is now serving fresh data!" "Green"
    Write-Log ""
    
    # Test production backend
    Write-Log "Testing production backend..." "Yellow"
    
    # Get VM external IP
    $VM_IP = gcloud compute instances describe dalal-street-backend --zone=us-central1-a --format="get(networkInterfaces[0].accessConfigs[0].natIP)"
    
    Write-Log "Production URL: http://$VM_IP:8000/health" "Cyan"
    
    try {
        $response = Invoke-RestMethod -Uri "http://$VM_IP:8000/health" -TimeoutSec 10
        if ($response.status -eq "healthy") {
            Write-Log "✓ Backend is healthy!" "Green"
            try {
                Invoke-RestMethod -Method POST -Uri "http://$VM_IP:8000/admin/update/resolver_cache" -TimeoutSec 10 | Out-Null
                Write-Log "✓ Resolver cache refreshed" "Green"
            } catch {
                Write-Log "⚠ Resolver cache refresh failed" "Yellow"
            }
        }
    } catch {
        Write-Log "⚠ Could not verify backend health (might still be starting)" "Yellow"
    }
    
} else {
    Write-Log "❌ VM update failed" "Red"
    exit 1
}

Write-Log ""
Write-Log "Update completed at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" "White"
