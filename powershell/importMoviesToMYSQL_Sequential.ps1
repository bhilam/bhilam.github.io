# ============================================================
# REQUIREMENTS
# ============================================================
if ($PSVersionTable.PSVersion.Major -lt 7) {
    Write-Host "❌ PowerShell 7+ required"
    exit
}

# ============================================================
# UTF-8 SAFETY
# ============================================================
chcp 65001 | Out-Null
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)

# ============================================================
# CONFIG
# ============================================================
$mysqlExe   = "C:\wamp64-3.3.7\bin\mysql\mysql9.1.0\bin\mysql.exe"
$sqlFolder  = "C:\wamp64-3.3.7\www\movies\antexport - copy"
$logFile    = "D:\entertainment\collecting\scripts\powershell\duplicates.log"

$mysqlHost  = "localhost"
$mysqlUser  = "root"
$mysqlDB    = "movies"

$throttle   = 4         # parallel files
$batchSize  = 50        # batch inserts

# ============================================================
# DUPLICATE LOG FILE
# ============================================================
if (-not (Test-Path $logFile)) { New-Item -ItemType File -Path $logFile -Force | Out-Null }

# ============================================================
# GET SQL FILES
# ============================================================
$sqlFiles = Get-ChildItem $sqlFolder -File |
    Where-Object { $_.Name -match '^movies_.*\.sql$' -and $_.Name -ne 'movies_00_0000.sql' } |
    Sort-Object Name

Write-Host "✔ Files to import: $($sqlFiles.Count)`n"

# ============================================================
# DROP & RECREATE TABLE
# ============================================================
$createTableSql = @"
DROP TABLE IF EXISTS movies;
CREATE TABLE movies (
  NUM INT NOT NULL AUTO_INCREMENT,
  CHECKED TEXT,
  COLORTAG TEXT,
  MEDIA TEXT,
  MEDIATYPE TEXT,
  SOURCE TEXT,
  DATEADDED DATE,
  BORROWER TEXT,
  DATEWATCHED DATE,
  USERRATING DECIMAL(3,1),
  RATING DECIMAL(3,1),
  ORIGINALTITLE TEXT,
  TRANSLATEDTITLE TEXT,
  FORMATTEDTITLE TEXT,
  DIRECTOR TEXT,
  PRODUCER TEXT,
  WRITER TEXT,
  COMPOSER TEXT,
  ACTORS TEXT,
  COUNTRY TEXT,
  YEAR INT,
  LENGTH INT,
  CATEGORY TEXT,
  CERTIFICATION TEXT,
  URL TEXT,
  DESCRIPTION TEXT,
  COMMENTS TEXT,
  FILEPATH TEXT,
  VIDEOFORMAT TEXT,
  VIDEOBITRATE TEXT,
  AUDIOFORMAT TEXT,
  AUDIOBITRATE TEXT,
  RESOLUTION TEXT,
  FRAMERATE TEXT,
  LANGUAGES TEXT,
  SUBTITLES TEXT,
  FILESIZE BIGINT,
  DISKS INT,
  PICTURESTATUS TEXT,
  NBEXTRAS INT,
  PICTURENAME TEXT,
  dupe_hash CHAR(64) GENERATED ALWAYS AS (
    SHA2(CONCAT_WS('|', FORMATTEDTITLE, YEAR, FILEPATH), 256)
  ) STORED,
  PRIMARY KEY (NUM),
  UNIQUE KEY uq_dupe (dupe_hash),
  FULLTEXT KEY ft_movies (FORMATTEDTITLE, ORIGINALTITLE, ACTORS, DIRECTOR, DESCRIPTION, COMMENTS)
) ENGINE=InnoDB CHARSET=utf8mb4;
"@

& $mysqlExe --host=$mysqlHost --user=$mysqlUser --database=$mysqlDB `
    --default-character-set=utf8mb4 --execute=$createTableSql 2>&1 | Out-Null

# ============================================================
# INSERT COLUMN LIST
# ============================================================
$columnsList = @"
CHECKED, COLORTAG, MEDIA, MEDIATYPE, SOURCE, DATEADDED, BORROWER, DATEWATCHED,
USERRATING, RATING, ORIGINALTITLE, TRANSLATEDTITLE, FORMATTEDTITLE, DIRECTOR,
PRODUCER, WRITER, COMPOSER, ACTORS, COUNTRY, YEAR, LENGTH, CATEGORY, CERTIFICATION,
URL, DESCRIPTION, COMMENTS, FILEPATH, VIDEOFORMAT, VIDEOBITRATE, AUDIOFORMAT,
AUDIOBITRATE, RESOLUTION, FRAMERATE, LANGUAGES, SUBTITLES, FILESIZE, DISKS,
PICTURESTATUS, NBEXTRAS, PICTURENAME
"@ -replace "\s+", " "

# ============================================================
# GLOBAL TRACKERS
# ============================================================
$globalStart    = Get-Date
$dashboard      = @{}
$globalTotal    = 0

foreach ($file in $sqlFiles) {
    $linesCount = (Get-Content $file.FullName -Encoding UTF8).Count
    $dashboard[$file.Name] = [PSCustomObject]@{
        Inserted = 0
        Failed   = 0
        Total    = $linesCount
        ETA      = [TimeSpan]::Zero
    }
    $globalTotal += $linesCount
}

# ============================================================
# PROCESS FILES
# ============================================================
foreach ($file in $sqlFiles) {

    $lines       = Get-Content $file.FullName -Encoding UTF8
    $buffer      = @()
    $inserted    = 0
    $totalRows   = $lines.Count
    $fileStart   = Get-Date

    for ($i = 0; $i -lt $lines.Count; $i++) {
        $line = $lines[$i].Trim()
        if ($line -match "^INSERT INTO movies.*VALUES\s*\((.*)\);$") {
            $buffer += "($($matches[1]))"
        }

        if ($buffer.Count -ge $batchSize) {
            $valuesSql = $buffer -join ",`n"
            $insertSql = @"
INSERT INTO movies ($columnsList)
VALUES
$valuesSql
ON DUPLICATE KEY UPDATE dupe_hash = dupe_hash;
"@
            & $mysqlExe --host=$mysqlHost --user=$mysqlUser --database=$mysqlDB `
                --default-character-set=utf8mb4 --execute="$insertSql" 2>&1 | Out-Null

            $inserted += $buffer.Count
            $buffer = @()
        }

        # --- Update dashboard ---
        $dashboard[$file.Name].Inserted = $inserted + $buffer.Count
        $dashboard[$file.Name].Failed   = $totalRows - ($inserted + $buffer.Count)

        # File ETA
        $processed = $dashboard[$file.Name].Inserted
        $elapsed   = (Get-Date) - $fileStart
        $dashboard[$file.Name].ETA = if ($processed -gt 0) {
            [TimeSpan]::FromSeconds((($totalRows - $processed) * ($elapsed.TotalSeconds / $processed)))
        } else { [TimeSpan]::Zero }

        # Global ETA
        $globalProcessed = ($dashboard.Values | Measure-Object Inserted -Sum).Sum
        $globalElapsed   = (Get-Date) - $globalStart
        $globalETA       = if ($globalProcessed -gt 0) {
            [TimeSpan]::FromSeconds((($globalTotal - $globalProcessed) * ($globalElapsed.TotalSeconds / $globalProcessed)))
        } else { [TimeSpan]::Zero }

        # --- Display dashboard ---
        Clear-Host
        Write-Host "=== GLOBAL PROGRESS ==="
        $percent = ($globalProcessed / $globalTotal) * 100
        Write-Host ("Global: {0:N2}% | ETA {1:00}:{2:00}:{3:00}" -f $percent, $globalETA.Hours, $globalETA.Minutes, $globalETA.Seconds)
        Write-Host ""

        foreach ($f in $sqlFiles) {
            $d = $dashboard[$f.Name]
            $p = ($d.Inserted / $d.Total) * 100
            $eta = $d.ETA
            Write-Host ("{0}: {1}/{2} rows | {3:N2}% | ETA {4:00}:{5:00}:{6:00}" -f $f.Name, $d.Inserted, $d.Total, $p, $eta.Hours, $eta.Minutes, $eta.Seconds)
        }
    }

    # --- Final batch for file ---
    if ($buffer.Count -gt 0) {
        $valuesSql = $buffer -join ",`n"
        $insertSql = @"
INSERT INTO movies ($columnsList)
VALUES
$valuesSql
ON DUPLICATE KEY UPDATE dupe_hash = dupe_hash;
"@
        & $mysqlExe --host=$mysqlHost --user=$mysqlUser --database=$mysqlDB `
            --default-character-set=utf8mb4 --execute="$insertSql" 2>&1 | Out-Null

        $inserted += $buffer.Count
        $dashboard[$file.Name].Inserted = $inserted
        $dashboard[$file.Name].Failed   = $totalRows - $inserted
        $buffer = @()
    }
}

# ============================================================
# FINAL SUMMARY
# ============================================================
$totalInserted = ($dashboard.Values | Measure-Object Inserted -Sum).Sum
$totalFailed   = ($dashboard.Values | Measure-Object Failed -Sum).Sum
$totalTime     = (Get-Date) - $globalStart

Write-Host "`n================ IMPORT SUMMARY ================"
$dashboard.GetEnumerator() | Sort-Object Name | Format-Table -AutoSize
Write-Host "Total inserted: $totalInserted"
Write-Host "Total failed  : $totalFailed"
Write-Host "Elapsed time  : $([math]::Round($totalTime.TotalMinutes,2)) minutes"
Write-Host "================================================"
