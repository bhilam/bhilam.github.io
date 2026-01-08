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
chcp 65001 | Out-Null
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

$DryRun = $true           # Set to $true to parse without inserting
$UpsertDuplicates = $true # If true, replace duplicate records

# =====================================================
# Helper Functions
# =====================================================
function Get-ElapsedSeconds([datetime]$startTime) {
    return ((Get-Date) - $startTime).TotalSeconds
}

function NormalizeLine($line) {
    return $line.Normalize([Text.NormalizationForm]::FormC)
}

function EscapeForMySql($value) {
    # Escape backslash and single quotes for MySQL
    return $value -replace "\\", "\\\\" -replace "'", "''"
}

function Get-RecordHash($line) {
    if ($line -match "VALUES\s*\((.*)\);$") {
        $valuesRaw = $matches[1]
        $pattern = ",(?=(?:[^']*'[^']*')*[^']*$)"
        $values = [regex]::Split($valuesRaw, $pattern)

        # Column indexes (0-based)
        $formattedTitle = NormalizeLine($values[13].Trim("'"))
        $director       = NormalizeLine($values[14].Trim("'"))
        $year           = $values[21].Trim("'")
        $url            = NormalizeLine($values[25].Trim("'"))
        $filepath       = NormalizeLine($values[28].Trim("'"))

        $hashString = "$formattedTitle|$year|$director|$filepath|$url"

        $sha = [System.Security.Cryptography.SHA256]::Create()
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($hashString)
        $hashBytes = $sha.ComputeHash($bytes)
        $hash = [BitConverter]::ToString($hashBytes) -replace '-', ''

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

function EscapeInsertLine($line) {
    return [regex]::Replace($line, "'([^']*)'", {
        param($matches)
        "'" + (EscapeForMySql($matches.Groups[1].Value)) + "'"
    })
}


# =====================================================
# Ensure Database Exists with utf8mb4
# =====================================================
if (-not $DryRun) {
    & $MySqlExe -h $MySqlHost -u $MySqlUser --execute "CREATE DATABASE IF NOT EXISTS $MySqlDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
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
# Auto-detect columns from first INSERT line
# =====================================================
$firstInsert = Get-Content $sqlFiles[0].FullName -Encoding UTF8 | Where-Object { $_ -match "^INSERT INTO movies" } | Select-Object -First 1

if (-not $firstInsert) {
    Write-Host "No INSERT statements found in the first SQL file." -ForegroundColor Red
    exit
}

# Extract column names
if ($firstInsert -match "INSERT INTO movies\s*\((.*?)\)\s*VALUES") {
    $columnsRaw = $matches[1]
    $columns = $columnsRaw -split ',' | ForEach-Object { $_.Trim() }
} else {
    Write-Host "Failed to parse column names." -ForegroundColor Red
    exit
}

# Define column types
$colDefs = @()
foreach ($col in $columns) {
    switch ($col) {
        "NUMBER" { $colDefs += "$col INT NOT NULL" }
        "CHECKED" { $colDefs += "$col BOOLEAN" }
        "COLORTAG" { $colDefs += "$col INT" }
        "MEDIA" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "MEDIATYPE" { $colDefs += "$col VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "SOURCE" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "DATEADDED" { $colDefs += "$col DATE" }
        "BORROWER" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "DATEWATCHED" { $colDefs += "$col DATE" }
        "USERRATING" { $colDefs += "$col VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "RATING" { $colDefs += "$col VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "ORIGINALTITLE" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "TRANSLATEDTITLE" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "FORMATTEDTITLE" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "DIRECTOR" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "PRODUCER" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "WRITER" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "COMPOSER" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "ACTORS" { $colDefs += "$col TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "COUNTRY" { $colDefs += "$col VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "YEAR" { $colDefs += "$col INT" }
        "LENGTH" { $colDefs += "$col INT" }
        "CATEGORY" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "CERTIFICATION" { $colDefs += "$col VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "URL" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "DESCRIPTION" { $colDefs += "$col TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "COMMENTS" { $colDefs += "$col TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "FILEPATH" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "VIDEOFORMAT" { $colDefs += "$col VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "VIDEOBITRATE" { $colDefs += "$col VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "AUDIOFORMAT" { $colDefs += "$col VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "AUDIOBITRATE" { $colDefs += "$col VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "RESOLUTION" { $colDefs += "$col VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "FRAMERATE" { $colDefs += "$col VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "LANGUAGES" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "SUBTITLES" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "FILESIZE" { $colDefs += "$col VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "DISKS" { $colDefs += "$col INT" }
        "PICTURESTATUS" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        "NBEXTRAS" { $colDefs += "$col INT" }
        "PICTURENAME" { $colDefs += "$col VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
        Default { $colDefs += "$col TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
    }
}

$createTableSQL = @"
CREATE TABLE IF NOT EXISTS movies (
    $($colDefs -join ",`n")
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"@

if (-not $DryRun) {
    & $MySqlExe -h $MySqlHost -u $MySqlUser --default-character-set=utf8mb4 $MySqlDB --execute $createTableSQL
    Write-Host "Table 'movies' ensured with utf8mb4 charset."
}

# =====================================================
# Initialize hash sets
# =====================================================
$existingHashes = New-Object System.Collections.Generic.HashSet[string]
$seenHashes     = New-Object System.Collections.Generic.HashSet[string]

# =====================================================
# Initialize logs and stats
# =====================================================
"==== Import started $(Get-Date) ====" | Out-File $LogFile -Encoding UTF8
"==== Duplicate detection log $(Get-Date) ====" | Out-File $DuplicateFile -Encoding UTF8

$totalFiles    = $sqlFiles.Count
$currentFile   = 0
$totalRecords  = 0
$duplicates    = 0
$totalBytes    = ($sqlFiles | Measure-Object Length -Sum).Sum
$processedBytes = 0
$globalStart   = Get-Date

# =====================================================
# Import loop (UTF-8 safe)
# =====================================================
foreach ($file in $sqlFiles) {

    $currentFile++
    $fileStart = Get-Date

    Write-Host ""
    Write-Host "[$currentFile/$totalFiles] Processing $($file.Name)"

    # Temp SQL file with UTF-8 encoding (no BOM)
    $tempSql = [System.IO.Path]::GetTempFileName()
    $writer = [System.IO.StreamWriter]::new($tempSql, $false, [System.Text.Encoding]::UTF8)
    $writer.WriteLine("START TRANSACTION;")

    # Open original SQL file as UTF-8
    $fs = [System.IO.File]::OpenRead($file.FullName)
    $reader = New-Object System.IO.StreamReader($fs, [System.Text.Encoding]::UTF8)

    $lineNumber = 0
    while (-not $reader.EndOfStream) {
        $lineNumber++
        $line = NormalizeLine($reader.ReadLine())

        if ($line -match "^INSERT INTO movies") {
            $record = Get-RecordHash $line
            if ($record -and ($existingHashes.Contains($record.Hash) -or $seenHashes.Contains($record.Hash))) {
                $duplicates++
                $logLine = "File: $($file.Name), Line: $lineNumber, FORMATTEDTITLE: '$($record.FormattedTitle)', YEAR: '$($record.Year)', DIRECTOR: '$($record.Director)', URL: '$($record.URL)', FILEPATH: '$($record.FilePath)', Hash: $($record.Hash)"
                $logLine | Out-File $DuplicateFile -Append -Encoding UTF8
                continue
            }

            $seenHashes.Add($record.Hash) | Out-Null

            # Upsert or ignore duplicates
            if ($UpsertDuplicates) {
                $line = $line -replace '^INSERT INTO movies', 'REPLACE INTO movies'
            } else {
                $line = $line -replace '^INSERT INTO movies', 'INSERT IGNORE INTO movies'
            }

            # Escape string literals for MySQL (UTF-8 safe)
            $line = EscapeInsertLine $line

            $writer.WriteLine($line)
            $totalRecords++
        } else {
            $writer.WriteLine($line)
        }

        # Progress tracking
        $processedBytes += $reader.CurrentEncoding.GetByteCount($line + "`n")
        $overallPct = [Math]::Min(100, ($processedBytes / $totalBytes) * 100)
        $filePct    = [Math]::Min(100, ($fs.Position / $fs.Length) * 100)

        Write-Progress -Id 1 -Activity "Overall Import Progress" -Status "File $currentFile of $totalFiles" -PercentComplete $overallPct
        Write-Progress -Id 2 -ParentId 1 -Activity "Importing $($file.Name)" -Status "Processing SQL..." -PercentComplete $filePct
    }

    $writer.WriteLine("COMMIT;")
    $reader.Close()
    $fs.Close()
    $writer.Close()

    # Execute the temp SQL file (UTF-8 safe)
    if (-not $DryRun) {
        & cmd /c "$MySqlExe --default-character-set=utf8mb4 -h $MySqlHost -u $MySqlUser $MySqlDB < `"$tempSql`""
        Start-Sleep -Milliseconds 200
    }

    Remove-Item $tempSql -Force -ErrorAction SilentlyContinue

    # Log speed
    $seconds = Get-ElapsedSeconds $fileStart
    $rate = if ($seconds -gt 0) { [math]::Round($totalRecords / $seconds, 2) } else { 0 }
    "$($file.Name) | Speed: $rate rows/sec" | Out-File $LogFile -Append -Encoding UTF8
}

# =====================================================
# Completion
# =====================================================
Write-Progress -Id 1 -Completed
Write-Progress -Id 2 -Completed

$elapsed = Get-ElapsedSeconds $globalStart

$summary = @"
==== Import completed $(Get-Date) ====
Files processed : $currentFile
Total records   : $totalRecords
Duplicates      : $duplicates
Time taken      : {0:N2} sec
Dry-run mode    : $DryRun
UPSERT enabled  : $UpsertDuplicates
UTF-8 safe      : Yes (preserves accents, emojis, other languages)
PowerShell 7+   : Required
Hash fields     : FORMATTEDTITLE | YEAR | DIRECTOR | FILEPATH | URL
Duplicate log  : $DuplicateFile
====================================
"@ -f $elapsed

$summary | Out-File $LogFile -Append -Encoding UTF8
Write-Host $summary -ForegroundColor Green

