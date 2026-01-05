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

$batchSize = 25
$throttle  = 4

if (-not (Test-Path $logFile)) { New-Item -ItemType File -Path $logFile -Force | Out-Null }
else { Clear-Content $logFile }

# ============================================================
# SQL FILES
# ============================================================
$sqlFiles = Get-ChildItem $sqlFolder -File |
    Where-Object { $_.Name -match '^movies_.*\.sql$' -and $_.Name -ne 'movies_00_0000.sql' } |
    Sort-Object Name

Write-Host "✔ Files to import: $($sqlFiles.Count)"

# ============================================================
# DROP & RECREATE TABLE
# ============================================================
$createSQL = @"
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
  dupe_hash CHAR(64) GENERATED ALWAYS AS (
      SHA2(CONCAT_WS('|', FORMATTEDTITLE, YEAR, FILEPATH), 256)
  ) STORED,
  PRIMARY KEY (NUM),
  UNIQUE KEY uq_dupe (dupe_hash),
  FULLTEXT KEY ft_movies (
    FORMATTEDTITLE, ORIGINALTITLE, ACTORS, DIRECTOR, DESCRIPTION, COMMENTS
  )
) ENGINE=InnoDB CHARSET=utf8mb4;
"@

& $mysqlExe --host=$mysqlHost --user=$mysqlUser --database=$mysqlDB --default-character-set=utf8mb4 --execute=$createSQL 2>&1 | Out-Null

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
# GLOBAL TRACKING
# ============================================================
$globalStart = Get-Date
$summary = @{}
foreach ($file in $sqlFiles) {
    $summary[$file.Name] = [PSCustomObject]@{
        Inserted = 0
        Failed   = 0
        Total    = 0
        Progress = 0
    }
}

# ============================================================
# PARALLEL PROCESSING WITH THREADJOBS
# ============================================================
$jobs = @()
foreach ($file in $sqlFiles) {
    $jobs += Start-ThreadJob -ScriptBlock {
        param($filePath, $mysqlExe, $mysqlHost, $mysqlUser, $mysqlDB, $columnsList, $logFile, $batchSize)

        $file = Get-Item $filePath
        $lines = Get-Content $file.FullName -Encoding UTF8
        $total = $lines.Count

        $batch = @()
        $inserted = 0
        $failed   = 0

        foreach ($line in $lines) {
            if ($line -match "^INSERT INTO movies.*VALUES\s*\((.*)\);$") {
                $batch += "($($matches[1]))"
            }

            if ($batch.Count -ge $batchSize) {
                $tmp = [System.IO.Path]::GetTempFileName() + ".sql"
                @"
INSERT IGNORE INTO movies ($columnsList)
VALUES
$($batch -join ",`n");
"@ | Set-Content -Path $tmp -Encoding UTF8

                $result = & $mysqlExe --host=$mysqlHost --user=$mysqlUser --database=$mysqlDB --default-character-set=utf8mb4 --execute="SOURCE $tmp;" 2>&1
                foreach ($lineOut in $result) {
                    if ($lineOut -match "Duplicate entry") {
                        Add-Content $logFile "$($file.Name) | $lineOut"
                        $failed++
                    }
                }
                $inserted += $batch.Count - $failed
                Remove-Item $tmp -Force
                $batch = @()
            }
        }

        if ($batch.Count -gt 0) {
            $tmp = [System.IO.Path]::GetTempFileName() + ".sql"
            @"
INSERT IGNORE INTO movies ($columnsList)
VALUES
$($batch -join ",`n");
"@ | Set-Content -Path $tmp -Encoding UTF8

            $result = & $mysqlExe --host=$mysqlHost --user=$mysqlUser --database=$mysqlDB --default-character-set=utf8mb4 --execute="SOURCE $tmp;" 2>&1
            foreach ($lineOut in $result) {
                if ($lineOut -match "Duplicate entry") {
                    Add-Content $logFile "$($file.Name) | $lineOut"
                    $failed++
                }
            }
            $inserted += $batch.Count - $failed
            Remove-Item $tmp -Force
        }

        return [PSCustomObject]@{
            File     = $file.Name
            Inserted = $inserted
            Failed   = $failed
            Total    = $total
        }

    } -ArgumentList $file.FullName, $mysqlExe, $mysqlHost, $mysqlUser, $mysqlDB, $columnsList, $logFile, $batchSize
}

# ============================================================
# LIVE DASHBOARD WITH COLORED PROGRESS BARS
# ============================================================
function Show-ProgressBar {
    param($percent, $width=30, $color1='Green', $color2='Red', $failPercent=0)

    $filled = [math]::Round($percent/100*$width)
    $failedBar = [math]::Round($failPercent/100*$width)
    $successBar = $filled - $failedBar
    $empty = $width - $filled

    $bar = ("#" * $successBar) + ("!" * $failedBar) + ("-" * ($empty))
    Write-Host -NoNewline "["
    Write-Host -NoNewline $bar -ForegroundColor $color1
    Write-Host "]"
}

while ($jobs | Where-Object { $_.State -eq 'Running' }) {
    foreach ($job in $jobs) {
        $res = Receive-Job -Job $job -Wait -AutoRemoveJob -ErrorAction SilentlyContinue
        if ($res) {
            $summary[$res.File].Inserted = $res.Inserted
            $summary[$res.File].Failed   = $res.Failed
            $summary[$res.File].Total    = $res.Total
            $summary[$res.File].Progress = [math]::Round(($res.Inserted + $res.Failed)/$res.Total*100,2)
        }
    }

    $totalFiles = $sqlFiles.Count
    $completedFiles = ($summary.Values | Where-Object { $_.Progress -eq 100 }).Count
    $totalInserted = ($summary.Values | Measure-Object Inserted -Sum).Sum
    $totalFailed   = ($summary.Values | Measure-Object Failed -Sum).Sum

    $elapsedGlobal = (Get-Date) - $globalStart
    $percentGlobal = [math]::Round(($completedFiles / $totalFiles)*100,2)
    $etaGlobal = if ($completedFiles -gt 0) {
        [TimeSpan]::FromSeconds((($totalFiles - $completedFiles) * ($elapsedGlobal.TotalSeconds / $completedFiles)))
    } else { [TimeSpan]::FromSeconds(0) }

    Clear-Host
    Write-Host ("Overall Progress: {0}/{1} files | {2}% | ETA: {3}" -f $completedFiles, $totalFiles, $percentGlobal, $etaGlobal.ToString('hh\:mm\:ss'))
    Write-Host "-------------------------------------------------------------------"

    $summary.GetEnumerator() | Sort-Object Name | ForEach-Object {
        $s = $_.Value
        $percentFailed = if ($s.Total -gt 0) { ($s.Failed / $s.Total) * 100 } else { 0 }
        $bar = Show-ProgressBar -percent $s.Progress -failPercent $percentFailed
        Write-Host ("{0,-30} {1} {2,6}% | Inserted: {3,5} | Failed: {4,5} | Total: {5,5}" -f $_.Key, "", $s.Progress, $s.Inserted, $s.Failed, $s.Total)
    }

    Start-Sleep -Milliseconds 500
}

# ============================================================
# FINAL SUMMARY
# ============================================================
$totalInserted = ($summary.Values | Measure-Object Inserted -Sum).Sum
$totalFailed   = ($summary.Values | Measure-Object Failed -Sum).Sum
$totalTime     = (Get-Date) - $globalStart

Write-Host "`n================ IMPORT SUMMARY ================"
Write-Host "Total inserted: $totalInserted"
Write-Host "Total failed   : $totalFailed"
Write-Host "Elapsed time   : $([math]::Round($totalTime.TotalMinutes,2)) minutes"
Write-Host "Duplicate log  : $logFile"
Write-Host "================================================"
