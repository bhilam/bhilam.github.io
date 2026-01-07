<#
.SYNOPSIS
Import CSV files into MySQL database with duplicate detection and UTF-8 handling.

.DESCRIPTION
- Adds a computed MD5 hash column to detect duplicates automatically.
- Supports dry-run, validation, UTF-8 safe import.
- Uses INSERT IGNORE to skip duplicates at MySQL level for large datasets.

.REQUIREMENTS
- PowerShell 7+
- MySQL CLI (mysql.exe)
- Database and table pre-created (optional drop)
- CSV files with semicolon delimiter
#>

# ========================
# Configuration
# ========================
$SourceFolder    = "C:\wamp64-3.3.7\www\movies\antexport - copy"
$FilePattern     = "movies_*_*-*.csv" 
$MySqlExe        = "C:\wamp64-3.3.7\bin\mysql\mysql9.1.0\bin\mysql.exe"
$MySqlLoginFile  = "C:\wamp64-3.3.7\bin\mysql\mysql9.1.0\mylogin.cnf"  # Secure credentials
$Database        = "movies"
$Table           = "movies"
$LogFile         = "import.log"
$DuplicateFile   = "duplicates.log"

$DropTable       = $false
$DryRun          = $false
$ValidationOnly  = $false

# Fields used to detect duplicates
$HashFields      = @("FORMATTEDTITLE", "YEAR", "DIRECTOR", "FILEPATH", "URL")

# ========================
# Initialization
# ========================
$files = Get-ChildItem -Path $SourceFolder -Filter $FilePattern
$totalRecords = 0
$duplicates = 0
$inserted = 0
$startTime = Get-Date

# Clear duplicate log
if (Test-Path $DuplicateFile) { Remove-Item $DuplicateFile -Force }

# ========================
# Ensure table exists (with optional drop)
# ========================
function Ensure-TableExists {
    param (
        [string]$Database,
        [string]$Table,
        [string]$MySqlExe,
        [string]$MySqlLoginFile,
        [switch]$DropFirst
    )

    if ($DropFirst) {
        Write-Host "Dropping table '$Table' if it exists..."
        $dropSql = "DROP TABLE IF EXISTS $Table;"
        & $MySqlExe --defaults-file="$MySqlLoginFile" --default-character-set=utf8mb4 $Database -e $dropSql
    }

    # Check if table exists
    $checkSql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$Database' AND table_name='$Table';"
    $tableExists = & $MySqlExe --defaults-file="$MySqlLoginFile" --skip-column-names -e $checkSql $Database

    if ($tableExists -eq "0") {
        Write-Host "Creating table '$Table' with hash column..."

        # Build comma-separated hash fields for SQL CONCAT
        $concatFields = $HashFields -join ", '|' ,"

        $createSql = @"
CREATE TABLE $Table (
    NUMBER INT NOT NULL,
    CHECKED BOOLEAN,
    COLORTAG INT,
    MEDIALABEL VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    MEDIATYPE VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    SOURCE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    DATE DATE,
    BORROWER VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    DATEWATCHED DATE,
    USERRATING VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    RATING VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    ORIGINALTITLE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    TRANSLATEDTITLE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    FORMATTEDTITLE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    DIRECTOR VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    PRODUCER VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    WRITER VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    COMPOSER VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    ACTORS TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    COUNTRY VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    YEAR INT,
    LENGTH INT,
    CATEGORY VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    CERTIFICATION VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    URL VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    DESCRIPTION TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    COMMENTS TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    FILEPATH VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    VIDEOFORMAT VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    VIDEOBITRATE VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    AUDIOFORMAT VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    AUDIOBITRATE VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    RESOLUTION VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    FRAMERATE VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    LANGUAGES VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    SUBTITLES VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    SIZE VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    DISKS INT,
    PICTURESTATUS VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    NBEXTRAS INT,
    ENumber VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    EChecked VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    ETag VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    ETitle VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    ECategory VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    EURL VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    EDescription VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    EComments VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    ECreatedBy VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    EPictureStatus VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    PICTURE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    record_hash CHAR(32) GENERATED ALWAYS AS (MD5(CONCAT_WS('|', $concatFields))) STORED,
    UNIQUE KEY unique_movie (record_hash)
);
"@

        & $MySqlExe --defaults-file="$MySqlLoginFile" --default-character-set=utf8mb4 $Database -e $createSql
        Write-Host "Table '$Table' created successfully with hash-based duplicate detection."
    } else {
        Write-Host "Table '$Table' already exists."
    }
}

# ========================
# Prepare table
# ========================
Ensure-TableExists -Database $Database -Table $Table -MySqlExe $MySqlExe -MySqlLoginFile $MySqlLoginFile -DropFirst:($DropTable)

# ========================
# Process each CSV file
# ========================
foreach ($currentFile in $files) {
    Write-Host "Processing file: $($currentFile.FullName)"

    $records = Import-Csv -Path $currentFile.FullName -Delimiter ";" -Encoding UTF8

    foreach ($record in $records) {
        $totalRecords++

        if ($DryRun -or $ValidationOnly) { 
            # Compute hash for logging only
            $concat = ($HashFields | ForEach-Object { $record."$_" }) -join "|"
            $hash = [System.BitConverter]::ToString(
                [System.Security.Cryptography.MD5]::Create().ComputeHash(
                    [System.Text.Encoding]::UTF8.GetBytes($concat)
                )
            )
            Add-Content -Path $DuplicateFile -Value $hash
            continue 
        }

        # Build MySQL INSERT command
        $columns = $record.PSObject.Properties.Name -join ", "
        $valuesArray = @()
        foreach ($prop in $record.PSObject.Properties) {
            $val = $prop.Value
            if ([string]::IsNullOrEmpty($val)) {
                $valuesArray += "NULL"
            } else {
                $escaped = $val -replace "'", "''"
                $valuesArray += "'$escaped'"
            }
        }
        $values = $valuesArray -join ", "

        # INSERT IGNORE automatically skips duplicates due to UNIQUE hash
        $sql = "INSERT IGNORE INTO $Table ($columns) VALUES ($values);"

        & $MySqlExe --defaults-file="$MySqlLoginFile" --default-character-set=utf8mb4 $Database -e $sql
        $inserted++
    }
}

# ========================
# Summary
# ========================
$elapsed = ((Get-Date) - $startTime).TotalSeconds
$summary = @"
========================= Import completed $(Get-Date) =========================
Files processed 		: $($files.Count)
Total records   		: $totalRecords
Inserted        		: $inserted
Duplicates skipped by MySQL	: $($totalRecords - $inserted)
Time taken      		: {0:N2} sec
Dry-run mode    		: $DryRun
Validation only 		: $ValidationOnly
UTF-8 safe      		: Yes
PowerShell 7+   		: Required
Hash fields     		: FORMATTEDTITLE | YEAR | DIRECTOR | FILEPATH | URL
Duplicate log   		: $DuplicateFile
================================================================================
"@ -f $elapsed

$summary | Out-File $LogFile -Append -Encoding UTF8
Write-Host $summary -ForegroundColor Green
