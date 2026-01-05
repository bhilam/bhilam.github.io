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
$mysqlExe  = "C:\wamp64-3.3.7\bin\mysql\mysql9.1.0\bin\mysql.exe"
$sqlFolder = "C:\wamp64-3.3.7\www\movies\antexport - copy"
$logFile   = "D:\entertainment\collecting\scripts\powershell\duplicates.log"

$mysqlHost = "localhost"
$mysqlUser = "root"
$mysqlDB   = "movies"

$batchSize = 25        # safe for huge rows
$throttle  = 4

if (-not (Test-Path $logFile)) {
    New-Item -ItemType File -Path $logFile -Force | Out-Null
}

# ============================================================
# SQL FILES
# ============================================================
$sqlFiles = Get-ChildItem $sqlFolder -File |
    Where-Object {
        $_.Name -match '^movies_.*\.sql$' -and $_.Name -ne 'movies_00_0000.sql'
    } |
    Sort-Object Name

Write-Host "✔ Files to import: $($sqlFiles.Count)"

# ============================================================
# DROP & RECREATE TABLE
# ============================================================
@"
DROP TABLE IF EXISTS movies;
CREATE TABLE movies (
  NUM INT NOT NULL,
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
  dupe_hash CHAR(64)
    GENERATED ALWAYS AS (
      SHA2(CONCAT_WS('|', FORMATTEDTITLE, YEAR, FILEPATH), 256)
    ) STORED,
  PRIMARY KEY (NUM),
  UNIQUE KEY uq_dupe (dupe_hash),
  FULLTEXT KEY ft_movies (
    FORMATTEDTITLE, ORIGINALTITLE, ACTORS, DIRECTOR, DESCRIPTION, COMMENTS
  )
) ENGINE=InnoDB CHARSET=utf8mb4;
"@ | & "$mysqlExe" --host=$mysqlHost --user=$mysqlUser --database=$mysqlDB `
     --default-character-set=utf8mb4 2>&1 | Out-Null

# ============================================================
# INSERT COLUMN LIST
# ============================================================
$columnsList = @"
NUM, CHECKED, COLORTAG, MEDIA, MEDIATYPE, SOURCE, DATEADDED, BORROWER, DATEWATCHED,
USERRATING, RATING, ORIGINALTITLE, TRANSLATEDTITLE, FORMATTEDTITLE, DIRECTOR,
PRODUCER, WRITER, COMPOSER, ACTORS, COUNTRY, YEAR, LENGTH, CATEGORY, CERTIFICATION,
URL, DESCRIPTION, COMMENTS, FILEPATH, VIDEOFORMAT, VIDEOBITRATE, AUDIOFORMAT,
AUDIOBITRATE, RESOLUTION, FRAMERATE, LANGUAGES, SUBTITLES, FILESIZE, DISKS,
PICTURESTATUS, NBEXTRAS, PICTURENAME
"@ -replace "\s+", " "

# ============================================================
# PARALLEL IMPORT (TEMP FILE BATCHING)
# ============================================================
$globalStart = Get-Date
$summary = [System.Collections.Concurrent.ConcurrentBag[object]]::new()

$sqlFiles | ForEach-Object -Parallel {

    $file = $_
    $lines = Get-Content $file.FullName -Encoding UTF8
    $total = $lines.Count

    $batch = @()
    $inserted = 0
    $failed   = 0
    $row = 0
    $start = Get-Date

    foreach ($line in $lines) {
        $row++

        $elapsed = (Get-Date) - $start
        $eta = if ($row -gt 0) {
            [TimeSpan]::FromSeconds(
                (($total - $row) * ($elapsed.TotalSeconds / $row))
            )
        }

        Write-Progress -Id $PID `
            -Activity "Importing $($file.Name)" `
            -Status "$row / $total | ETA $($eta.ToString('hh\:mm\:ss'))" `
            -PercentComplete (($row / $total) * 100)

        if ($line -match "^INSERT INTO movies.*VALUES\s*\((.*)\);$") {
            $batch += "($($matches[1]))"
        }

        if ($batch.Count -ge $using:batchSize) {
            $tmp = [System.IO.Path]::GetTempFileName() + ".sql"

            @"
INSERT IGNORE INTO movies ($using:columnsList)
VALUES
$($batch -join ",`n");
"@ | Set-Content -Path $tmp -Encoding UTF8

            & "$using:mysqlExe" `
                --host=$using:mysqlHost --user=$using:mysqlUser `
                --database=$using:mysqlDB `
                --default-character-set=utf8mb4 `
                --execute="SOURCE $tmp;" 2>&1 | Out-Null

            $inserted += $batch.Count
            Remove-Item $tmp -Force
            $batch = @()
        }
    }

    if ($batch.Count -gt 0) {
        $tmp = [System.IO.Path]::GetTempFileName() + ".sql"

        @"
INSERT IGNORE INTO movies ($using:columnsList)
VALUES
$($batch -join ",`n");
"@ | Set-Content -Path $tmp -Encoding UTF8

        & "$using:mysqlExe" `
            --host=$using:mysqlHost --user=$using:mysqlUser `
            --database=$using:mysqlDB `
            --default-character-set=utf8mb4 `
            --execute="SOURCE $tmp;" 2>&1 | Out-Null

        $inserted += $batch.Count
        Remove-Item $tmp -Force
    }

    [PSCustomObject]@{
        File     = $file.Name
        Inserted = $inserted
        Failed   = ($total - $inserted)
    }

} -ThrottleLimit $throttle |
ForEach-Object { $summary.Add($_) }

# ============================================================
# SUMMARY
# ============================================================
$totalTime = (Get-Date) - $globalStart

Write-Host "`n================ IMPORT SUMMARY ================"
$summary | Sort-Object File | Format-Table -AutoSize
Write-Host "Total inserted: $(($summary | Measure-Object Inserted -Sum).Sum)"
Write-Host "Elapsed time : $([math]::Round($totalTime.TotalMinutes,2)) minutes"
Write-Host "================================================"
