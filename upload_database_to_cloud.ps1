# Configuration
$BUCKET_NAME = "dalal-street-database-storage"
$DB_PATH = "e:\Dalal Street Trae\App\database\stock_market_new.db"
$TIMESTAMP = Get-Date -Format "yyyy-MM-dd_HH-mm"

# Pre-upload validator: parse latest AUTHORITATIVE_DAILY runner summary
function Test-RunnerSummary {
    $MIN_STOCKS_MASTER = 1000
    $MIN_DAILY_OHLC_ROWS = 1500
    $MIN_FUNDAMENTALS_TOTAL = 1000
    $MIN_FUNDAMENTALS_UPDATED_TODAY = 50
    $logDir = "e:\Dalal Street Trae\logs"
    $latestLog = Get-ChildItem -Path $logDir -Filter "AUTHORITATIVE_DAILY_*.log" | Sort-Object LastWriteTime | Select-Object -Last 1
    if (-not $latestLog) { Write-Host "❌ No runner log found" -ForegroundColor Red; return $false }
    $lines = Get-Content $latestLog.FullName | Select-String -Pattern "^SUMMARY_JSON\s+\{" | Select-Object -Last 1
    if (-not $lines) { Write-Host "❌ SUMMARY_JSON not found in $($latestLog.Name)" -ForegroundColor Red; return $false }
    $json = $lines.ToString().Substring($lines.ToString().IndexOf('{'))
    $obj = $json | ConvertFrom-Json
    $critOk = $true
    Write-Host "Validator: Runner summary" -ForegroundColor Cyan
    Write-Host ("stocks_master: {0}" -f $obj.stocks_master)
    Write-Host ("daily_ohlc_rows_for_date: {0}" -f $obj.daily_ohlc_rows_for_date)
    Write-Host ("fundamentals_total: {0}" -f $obj.fundamentals_total)
    Write-Host ("fundamentals_updated_today: {0}" -f $obj.fundamentals_updated_today)
    if ([int]$obj.stocks_master -lt $MIN_STOCKS_MASTER) { Write-Host ("FAIL stocks_master < {0}" -f $MIN_STOCKS_MASTER) -ForegroundColor Red; $critOk = $false }
    if ([int]$obj.daily_ohlc_rows_for_date -lt $MIN_DAILY_OHLC_ROWS) { Write-Host ("FAIL daily_ohlc_rows_for_date < {0}" -f $MIN_DAILY_OHLC_ROWS) -ForegroundColor Red; $critOk = $false }
    if ([int]$obj.fundamentals_total -lt $MIN_FUNDAMENTALS_TOTAL) { Write-Host ("FAIL fundamentals_total < {0}" -f $MIN_FUNDAMENTALS_TOTAL) -ForegroundColor Red; $critOk = $false }
    if ([int]$obj.fundamentals_updated_today -lt $MIN_FUNDAMENTALS_UPDATED_TODAY) { Write-Host ("WARN fundamentals_updated_today < {0}" -f $MIN_FUNDAMENTALS_UPDATED_TODAY) -ForegroundColor Yellow }
    if (-not $obj.cf_ca_csv) { Write-Host "WARN CF-CA CSV not detected in runner (will proceed)" -ForegroundColor Yellow }
    if (-not $obj.market_indices_last_date) { Write-Host "WARN market indices missing (non-critical)" -ForegroundColor Yellow }
    if (-not $obj.fii_dii_last_date) { Write-Host "WARN FII/DII missing (non-critical)" -ForegroundColor Yellow }
    if (-not $critOk) { Write-Host "❌ Pre-upload validation failed" -ForegroundColor Red }
    else { Write-Host "✓ Pre-upload validation passed" -ForegroundColor Green }
    return $critOk
}

Write-Host "=================================================" -ForegroundColor Cyan
Write-Host "Database Upload Script - $TIMESTAMP" -ForegroundColor Cyan
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host ""

# Check if database exists
if (-Not (Test-Path $DB_PATH)) {
    Write-Host "❌ Database not found at: $DB_PATH" -ForegroundColor Red
    exit 1
}

# Get database size
$dbSize = (Get-Item $DB_PATH).Length / 1MB
Write-Host "[INFO] Database size: $([math]::Round($dbSize, 2)) MB" -ForegroundColor Yellow
Write-Host ""

# Upload to Cloud Storage
if (-not (Test-RunnerSummary)) { exit 1 }
Write-Host "[1/3] Uploading database to Cloud Storage..." -ForegroundColor Green
gsutil -m cp "$DB_PATH" "gs://$BUCKET_NAME/stock_market_new.db"

if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Upload successful" -ForegroundColor Green
    Write-Host ""
    
    # Create timestamped backup
    Write-Host "[2/3] Creating timestamped backup..." -ForegroundColor Green
    gsutil cp "gs://$BUCKET_NAME/stock_market_new.db" "gs://$BUCKET_NAME/backups/stock_market_$TIMESTAMP.db"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Backup created: stock_market_$TIMESTAMP.db" -ForegroundColor Green
        Write-Host ""
        Write-Host "=================================================" -ForegroundColor Cyan
        Write-Host "✅ DATABASE UPLOADED SUCCESSFULLY!" -ForegroundColor Green
        Write-Host "=================================================" -ForegroundColor Cyan
        
        # Upload latest CF-CA CSV (if present)
        $CFCA = Get-ChildItem -Path "e:\Dalal Street Trae\App\database" -Filter "CF-CA*.csv" | Sort-Object LastWriteTime | Select-Object -Last 1
        if ($CFCA) {
            Write-Host "[3/3] Uploading CF-CA CSV: $($CFCA.Name)" -ForegroundColor Green
            gsutil -m cp "$($CFCA.FullName)" "gs://$BUCKET_NAME/$($CFCA.Name)"
            gsutil -m cp "$($CFCA.FullName)" "gs://$BUCKET_NAME/backups/cfca/$($TIMESTAMP)-$($CFCA.Name)"
        } else {
            Write-Host "⚠ No CF-CA CSV found in App\\database" -ForegroundColor Yellow
        }
        Write-Host ""
        Write-Host "Next steps:" -ForegroundColor Yellow
        Write-Host "1. SSH into your VM" -ForegroundColor White
        Write-Host "2. Run: ~/download_database.sh" -ForegroundColor White
        Write-Host ""
        Write-Host "Or use the complete update script!" -ForegroundColor Yellow
    } else {
        Write-Host "⚠ Backup creation failed (main upload succeeded)" -ForegroundColor Yellow
    }
} else {
    Write-Host "❌ Upload failed" -ForegroundColor Red
    exit 1
}