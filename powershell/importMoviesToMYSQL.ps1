# =====================================================
# PowerShell Version Check
# =====================================================
if ($PSVersionTable.PSVersion.Major -lt 7) {
    Write-Host "This script requires PowerShell 7 or higher." -ForegroundColor Red
    exit
}

# =====================================================
# UTF-8 SAFETY
# =====================================================
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)

# =====================================================
# Configuration
# =====================================================
$SourceFolder = "C:\wamp64-3.3.7\www\movies\antexport - copy"

$MySqlExe  = "C:\wamp64-3.3.7\bin\mysql\mysql9.1.0\bin\mysql.exe"
$MySqlHost = "localhost"
$MySqlUser = "root"
$MySqlDB   = "movies"

$LogFile       = Join-Path $SourceFolder "import_log.txt"
$DuplicateFile = Join-Path $SourceFolder "duplicates_log.txt"

$DryRun = $true            # Set to $false to actually import
$UpsertDuplicates = $true  # REPLACE INTO vs INSERT IGNORE

# =====================================================
# Helper Functions
# =====================================================
function Get-ElapsedSeconds([datetime]$startTime) {
    ((Get-Date) - $startTime).TotalSeconds
}

function NormalizeLine($line) {
    $line.Normalize([Text.NormalizationForm]::FormC)
}

function Get-RecordHash($line) {
    if ($line -match "VALUES\s*\((.*)\);$") {
        $valuesRaw = $matches[1]
        $pattern = ",(?=(?:[^']*'[^']*')*[^']*$)"
        $values = [regex]::Split($valuesRaw, $pattern)

        $formattedTitle = NormalizeLine($values[13].Trim("'"))
        $director       = NormalizeLine($values[14].Trim("'"))
        $year           = $values[21].Trim("'")
        $url            = NormalizeLine($values[25].Trim("'"))
        $filepath       = NormalizeLine($values[28].Trim("'"))

        $hashString = "$formattedTitle|$year|$director|$filepath|$url"

        $sha = [System.Security.Cryptography.SHA256]::Create()
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($hashString)
        $hash = [BitConverter]::ToString($sha.ComputeHash($bytes)) -replace '-', ''

        return @{
            Hash = $hash
            FormattedTitle = $formattedTitle
            Year = $year
            Director = $director
            FilePath = $filepath
            URL = $url
        }
    }
    return $null
}

# =====================================================
# Ensure Database Exists (utf8mb4)
# =====================================================
if (-not $DryRun) {
    & $MySqlExe `
        -h $MySqlHost `
        -u $MySqlUser `
        --execute "CREATE DATABASE IF NOT EXISTS $MySqlDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
}

# =====================================================
# Locate SQL files
# =====================================================
$sqlFiles = Get-ChildItem -Path $SourceFolder -Filter "movies_*.sql" |
    Where-Object { $_.Name -like 'movies_*_*-*.sql' -and $_.Name -ne 'movies_00_0000.sql' } |
    Sort-Object Name

if ($sqlFiles.Count -eq 0) {
    Write-Host "No matching SQL files found." -ForegroundColor Yellow
    exit
}

# =====================================================
# Hash Sets
# =====================================================
$existingHashes = [System.Collections.Generic.HashSet[string]]::new()
$seenHashes     = [System.Collections.Generic.HashSet[string]]::new()

# =====================================================
# Logs
# =====================================================
"==== Import started $(Get-Date) ====" | Out-File $LogFile -Encoding UTF8
"==== Duplicate log $(Get-Date) ====" | Out-File $DuplicateFile -Encoding UTF8

$totalFiles = $sqlFiles.Count
$currentFile = 0
$totalRecords = 0
$duplicates = 0
$globalStart = Get-Date

# =====================================================
# Import Loop (direct piping, UTF-8 safe)
# =====================================================
foreach ($file in $sqlFiles) {

    $currentFile++
    Write-Host "`n[$currentFile/$totalFiles] Processing $($file.Name)"

    $linesToSend = @()
    $linesToSend += "START TRANSACTION;"
    $linesToSend += "SET NAMES utf8mb4;"
    $linesToSend += "SET character_set_client = utf8mb4;"
    $linesToSend += "SET character_set_connection = utf8mb4;"
    $linesToSend += "SET character_set_results = utf8mb4;"

    Get-Content $file.FullName -Encoding UTF8 | ForEach-Object {
        $line = NormalizeLine($_)

        if ($line -match "^INSERT INTO movies") {
            $record = Get-RecordHash $line

            if ($record -and ($existingHashes.Contains($record.Hash) -or $seenHashes.Contains($record.Hash))) {
                $duplicates++
                "Duplicate: $($record.FormattedTitle) ($($record.Year))" |
                    Out-File $DuplicateFile -Append -Encoding UTF8
                return
            }

            if ($record) {
                $seenHashes.Add($record.Hash) | Out-Null
            }

            if ($UpsertDuplicates) {
                $line = $line -replace '^INSERT INTO movies', 'REPLACE INTO movies'
            } else {
                $line = $line -replace '^INSERT INTO movies', 'INSERT IGNORE INTO movies'
            }

            $totalRecords++
        }

        $linesToSend += $line
    }

    $linesToSend += "COMMIT;"

    # Send SQL directly to MySQL
    if (-not $DryRun) {
        $linesToSend -join "`n" | & $MySqlExe `
            -h $MySqlHost `
            -u $MySqlUser `
            --database=$MySqlDB `
            --default-character-set=utf8mb4
    }
}

# =====================================================
# Completion
# =====================================================
$elapsed = Get-ElapsedSeconds $globalStart

$summary = @"
==== Import completed $(Get-Date) ====
Files processed : $currentFile
Total records   : $totalRecords
Duplicates      : $duplicates
Time taken      : {0:N2} sec
Dry-run mode    : $DryRun
UPSERT enabled  : $UpsertDuplicates
UTF-8 safe      : YES (accented & multilingual)
PowerShell 7+   : Required
Hash fields     : FORMATTEDTITLE | YEAR | DIRECTOR | FILEPATH | URL
Duplicate log  : $DuplicateFile
====================================
"@ -f $elapsed

$summary | Out-File $LogFile -Append -Encoding UTF8
Write-Host $summary -ForegroundColor Green
