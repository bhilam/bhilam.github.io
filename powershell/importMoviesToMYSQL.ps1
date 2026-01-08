# =====================================================
# PowerShell Version Check
# =====================================================
if ($PSVersionTable.PSVersion.Major -lt 7) {
    Write-Host "This script requires PowerShell 7 or higher." -ForegroundColor Red
    exit
}

# =====================================================
# UTF-8 + LATIN1 SAFETY
# =====================================================
chcp 65001 | Out-Null
$Utf8NoBom      = [System.Text.UTF8Encoding]::new($false)
$Latin1Encoding = [System.Text.Encoding]::GetEncoding("ISO-8859-1")
$OutputEncoding = $Utf8NoBom

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

$DryRun = $false
$UpsertDuplicates = $true

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
    #return $value -replace "\\", "\\\\" -replace "'", "''"
	# Only escape single quotes for MySQL
    return $value -replace "'", "''"
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
# Ensure Database Exists (utf8mb4)
# =====================================================
if (-not $DryRun) {
    & $MySqlExe -h $MySqlHost -u $MySqlUser `
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
# Auto-detect columns
# =====================================================
$firstInsert = Get-Content $sqlFiles[0].FullName -Encoding ISO-8859-1 |
    Where-Object { $_ -match "^INSERT INTO movies" } |
    Select-Object -First 1

if (-not $firstInsert) {
    Write-Host "No INSERT statements found." -ForegroundColor Red
    exit
}

if ($firstInsert -match "INSERT INTO movies\s*\((.*?)\)\s*VALUES") {
    $columns = $matches[1] -split ',' | ForEach-Object { $_.Trim() }
} else {
    Write-Host "Failed to parse column names." -ForegroundColor Red
    exit
}

# =====================================================
# Column Definitions (unchanged)
# =====================================================
$colDefs = @()
foreach ($col in $columns) {
    switch ($col) {
        "NUM" { $colDefs += "$col INT NOT NULL" }
        "CHECKED" { $colDefs += "$col BOOLEAN" }
        "COLORTAG" { $colDefs += "$col INT" }
        "YEAR" { $colDefs += "$col INT" }
        "LENGTH" { $colDefs += "$col INT" }
        "DISKS" { $colDefs += "$col INT" }
        "NBEXTRAS" { $colDefs += "$col INT" }
        Default { $colDefs += "$col TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" }
    }
}

$createTableSQL = @"
CREATE TABLE IF NOT EXISTS movies (
    $($colDefs -join ",`n")
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"@

if (-not $DryRun) {
    & $MySqlExe -h $MySqlHost -u $MySqlUser `
        --default-character-set=utf8mb4 $MySqlDB `
        --execute $createTableSQL
}

# =====================================================
# Hash Sets
# =====================================================
$existingHashes = New-Object System.Collections.Generic.HashSet[string]
$seenHashes     = New-Object System.Collections.Generic.HashSet[string]

# =====================================================
# Logs & Stats
# =====================================================
"==== Import started $(Get-Date) ====" | Out-File $LogFile -Encoding UTF8
"==== Duplicate detection log $(Get-Date) ====" | Out-File $DuplicateFile -Encoding UTF8

$totalFiles      = $sqlFiles.Count
$currentFile     = 0
$totalRecords    = 0
$duplicates      = 0
$totalBytes      = ($sqlFiles | Measure-Object Length -Sum).Sum
$processedBytes  = 0
$globalStart     = Get-Date

# =====================================================
# Import Loop (LATIN1 → UTF-8)
# =====================================================
foreach ($file in $sqlFiles) {

    $currentFile++
    $fileStart = Get-Date

    Write-Host ""
    Write-Host "[$currentFile/$totalFiles] Processing $($file.Name)"

    $tempSql = [System.IO.Path]::GetTempFileName()
    $writer  = [System.IO.StreamWriter]::new($tempSql, $false, $Utf8NoBom)
    $writer.WriteLine("START TRANSACTION;")

    $fs     = [System.IO.File]::OpenRead($file.FullName)
    $reader = New-Object System.IO.StreamReader($fs, $Latin1Encoding)

    $lineNumber = 0
    while (-not $reader.EndOfStream) {
        $lineNumber++
        $rawLine = $reader.ReadLine()
        $line = NormalizeLine($rawLine)

        if ($line -match "^INSERT INTO movies") {
            $record = Get-RecordHash $line
            if ($record -and ($existingHashes.Contains($record.Hash) -or $seenHashes.Contains($record.Hash))) {
                $duplicates++
                "File:$($file.Name) Line:$lineNumber Hash:$($record.Hash)" |
                    Out-File $DuplicateFile -Append -Encoding UTF8
                continue
            }

            $seenHashes.Add($record.Hash) | Out-Null

            if ($UpsertDuplicates) {
                $line = $line -replace '^INSERT INTO movies', 'REPLACE INTO movies'
            } else {
                $line = $line -replace '^INSERT INTO movies', 'INSERT IGNORE INTO movies'
            }

            $line = EscapeInsertLine $line
            $writer.WriteLine($line)
            $totalRecords++
        } else {
            $writer.WriteLine($line)
        }

        $processedBytes += $Latin1Encoding.GetByteCount($rawLine + "`n")

        Write-Progress -Id 1 -Activity "Overall Import" `
            -PercentComplete ([Math]::Min(100, ($processedBytes / $totalBytes) * 100))
    }

    $writer.WriteLine("COMMIT;")
    $reader.Close()
    $fs.Close()
    $writer.Close()

    if (-not $DryRun) {
        & cmd /c "$MySqlExe --default-character-set=utf8mb4 -h $MySqlHost -u $MySqlUser $MySqlDB < `"$tempSql`""
    }

    Remove-Item $tempSql -Force -ErrorAction SilentlyContinue
}

# =====================================================
# Completion
# =====================================================
Write-Progress -Id 1 -Completed

$elapsed = Get-ElapsedSeconds $globalStart

$summary = @"
==== Import completed $(Get-Date) ====
Files processed : $currentFile
Total records   : $totalRecords
Duplicates      : $duplicates
Time taken      : {0:N2} sec
Dry-run mode    : $DryRun
UPSERT enabled  : $UpsertDuplicates
Input charset   : latin1 (ISO-8859-1)
Output charset  : utf8mb4
====================================
"@ -f $elapsed

$summary | Out-File $LogFile -Append -Encoding UTF8
Write-Host $summary -ForegroundColor Green
