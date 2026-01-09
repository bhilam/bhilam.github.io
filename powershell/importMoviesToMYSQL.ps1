# =====================================================
# ================= Configuration ====================
# =====================================================
$global:committedRecords = 0

$Config = @{
    SourceFolder       = "C:\wamp64-3.3.7\www\movies\antexport - copy"
    MySqlExe           = "C:\wamp64-3.3.7\bin\mysql\mysql9.1.0\bin\mysql.exe"
    MySqlHost          = "localhost"
    MySqlUser          = "root"
    MySqlPass          = ""
    MySqlDB            = "movies"
    LogFile            = "import_log.txt"
    DuplicateFile      = "duplicates_log.txt"
    DryRun             = $false
    DropTable          = $true
    InputEncoding      = [Text.Encoding]::GetEncoding("ISO-8859-1")
    OutputEncoding     = [Text.UTF8Encoding]::new($false)
    SqlFileFilter      = "movies_*_*-*.sql"
    ExcludeFiles       = @('movies_00_0000.sql')
    BatchSize          = 500
}

$Config.LogFile       = Join-Path $Config.SourceFolder $Config.LogFile
$Config.DuplicateFile = Join-Path $Config.SourceFolder $Config.DuplicateFile

# =====================================================
# ================= PowerShell Check =================
# =====================================================
if ($PSVersionTable.PSVersion.Major -lt 7) {
    Write-Host "PowerShell 7+ required." -ForegroundColor Red
    exit
}

chcp 65001 | Out-Null
$OutputEncoding = $Config.OutputEncoding

# =====================================================
# ================= Helper Functions =================
# =====================================================
function NormalizeLine($line) {
    $line.Normalize([Text.NormalizationForm]::FormC)
}

function Get-ElapsedSeconds($start) {
    ((Get-Date) - $start).TotalSeconds
}

function Get-RecordHash($v) {
    $s = "$($v.FORMATTEDTITLE)|$($v.YEAR)|$($v.DIRECTOR)|$($v.URL)|$($v.FILEPATH)"
    $sha = [Security.Cryptography.SHA256]::Create()
    ([BitConverter]::ToString(
        $sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($s))
    )) -replace '-', ''
}

function ParseInsertLine {
    param ($line, $Columns)

    if ($line -notmatch "^INSERT INTO movies\s*\(.+?\)\s*VALUES\s*\(.+\);$") {
        return $null
    }

    $vals = [regex]::Split(
        ($line -replace '^.+?VALUES\s*\(|\);$',''),
        ",(?=(?:[^']*'[^']*')*[^']*$)"
    )

    if ($vals.Count -ne $Columns.Count) { return $null }

    $row = @{ }
    for ($i=0; $i -lt $Columns.Count; $i++) {
        $v = $vals[$i].Trim()
        if ($v -eq "NULL") { $v = $null }
        elseif ($v.StartsWith("'")) {
            $v = $v.Substring(1,$v.Length-2).Replace("''","'")
        }
        $row[$Columns[$i]] = $v
    }
    return $row
}

# =====================================================
# ================= Locate SQL Files =================
# =====================================================
$sqlFiles = Get-ChildItem $Config.SourceFolder -Filter $Config.SqlFileFilter |
            Where-Object { $Config.ExcludeFiles -notcontains $_.Name } |
            Sort-Object Name

if (-not $sqlFiles) {
    Write-Host "No SQL files found." -ForegroundColor Yellow
    exit
}

# =====================================================
# ================= Detect Columns ===================
# =====================================================
$firstInsert = Get-Content $sqlFiles[0].FullName -Encoding $Config.InputEncoding |
               Where-Object { $_ -match "INSERT INTO movies" } |
               Select-Object -First 1

$Columns = ($firstInsert -replace '^.+?\(|\)\s*VALUES.+$','') -split '\s*,\s*'

# =====================================================
# ================= Duplicate Tracking ===============
# =====================================================
$seenHashes = [System.Collections.Generic.HashSet[string]]::new()

# =====================================================
# ================= Database Setup ===================
# =====================================================
if (-not $Config.DryRun) {

    & $Config.MySqlExe -u $Config.MySqlUser `
        -e "CREATE DATABASE IF NOT EXISTS $($Config.MySqlDB) CHARACTER SET utf8mb4;"

    if ($Config.DropTable) {
        & $Config.MySqlExe -u $Config.MySqlUser $Config.MySqlDB `
            -e "DROP TABLE IF EXISTS movies;"
    }

    $defs = @()
    foreach ($c in $Columns) {
        if ($c -eq "NUM") {
            $defs += "$c INT NOT NULL PRIMARY KEY"
        } else {
            $defs += "$c TEXT CHARACTER SET utf8mb4"
        }
    }
    $defs += "RECORDHASH CHAR(64) NOT NULL UNIQUE"

    & $Config.MySqlExe -u $Config.MySqlUser $Config.MySqlDB `
        -e "CREATE TABLE IF NOT EXISTS movies ($($defs -join ',')) ENGINE=InnoDB;"
}

# =====================================================
# ================= MySQL Connection =================
# =====================================================
if (-not $Config.DryRun) {
    Add-Type -Path "C:\Program Files (x86)\MySQL\MySQL Connector NET 9.5\MySql.Data.dll"
    $conn = New-Object MySql.Data.MySqlClient.MySqlConnection(
        "server=$($Config.MySqlHost);uid=$($Config.MySqlUser);database=$($Config.MySqlDB);charset=utf8mb4;"
    )
    $conn.Open()
    $tx = $conn.BeginTransaction()
}

# =====================================================
# ================= Batch Insert =====================
# =====================================================
function InsertBatch($rows) {
    if (-not $rows -or $Config.DryRun) { return }

    $cols = $Columns + "RECORDHASH"
    $cmd  = $conn.CreateCommand()
    $cmd.Transaction = $tx

    $valuesList = @()

    for ($i=0; $i -lt $rows.Count; $i++) {
        $paramNames = @()
        foreach ($c in $cols) {
            $paramName = "@${c}_$i"
            $paramNames += $paramName
            $value = $rows[$i][$c]
            if ($null -eq $value) { $value = [DBNull]::Value }
            $cmd.Parameters.AddWithValue($paramName, $value) | Out-Null
        }
        $valuesList += "(" + ($paramNames -join ',') + ")"
    }

    $columnList = ($cols -join ',')
    $cmd.CommandText = "INSERT INTO movies ($columnList) VALUES " + ($valuesList -join ',')

    $global:committedRecords += $cmd.ExecuteNonQuery()
}

# =====================================================
# ================= Import Loop ======================
# =====================================================
$totalRecords = 0
$duplicates   = 0
$currentFile  = 0
$globalStart  = Get-Date
$totalFiles   = $sqlFiles.Count

foreach ($file in $sqlFiles) {
    $currentFile++
    $reader = [IO.StreamReader]::new($file.FullName,$Config.InputEncoding)
    $batch  = @()
    $lineNo = 0

    # Count total lines for per-file progress
    $totalLines = (Get-Content $file.FullName -Encoding $Config.InputEncoding).Count

    while (-not $reader.EndOfStream) {
        $lineNo++
        $row = ParseInsertLine (NormalizeLine $reader.ReadLine()) $Columns
        if (-not $row) { continue }

        $totalRecords++
        $hash = Get-RecordHash $row

        if ($seenHashes.Contains($hash)) {
            $duplicates++
            "File:$($file.Name) Line:$lineNo Hash:$hash" |
                Out-File $Config.DuplicateFile -Append
            continue
        }

        $seenHashes.Add($hash) | Out-Null
        $row.RECORDHASH = $hash
        $batch += $row

        if ($batch.Count -ge $Config.BatchSize) {
            InsertBatch $batch
            $batch = @()
        }

        # Per-file progress
        $percentFile = [math]::Round(($lineNo / $totalLines) * 100, 0)
        Write-Progress -Activity "Processing file $($file.Name)" `
                       -Status "$lineNo / $totalLines lines" `
                       -PercentComplete $percentFile `
                       -Id 1
    }

    InsertBatch $batch
    $reader.Close()

    # Overall progress
    $percentTotal = [math]::Round(($currentFile / $totalFiles) * 100, 0)
    Write-Progress -Activity "Overall Progress" `
                   -Status "File $currentFile / $totalFiles processed" `
                   -PercentComplete $percentTotal `
                   -Id 0
}

if (-not $Config.DryRun) {
    $tx.Commit()
    $conn.Close()
}

# =====================================================
# ================= Summary ==========================
# =====================================================
$elapsed = Get-ElapsedSeconds $globalStart

$summary = @"
============== Import completed $(Get-Date) ==============
Files processed          : $currentFile
Total records parsed     : $totalRecords
Records committed        : $committedRecords
Duplicates logged        : $duplicates
Hash fields              : FORMATTEDTITLE | YEAR | DIRECTOR | URL | FILEPATH
Time taken               : {0:N2} sec
Dry-run mode             : $($Config.DryRun)
Drop table before import : $($Config.DropTable)
Input charset            : latin1 (ISO-8859-1)
Output charset           : utf8mb4
SQL file filter          : $($Config.SqlFileFilter)
Excluded files           : $($Config.ExcludeFiles -join ', ')
=========================================================
"@ -f $elapsed

$summary | Out-File $Config.LogFile -Append -Encoding UTF8
Write-Host $summary -ForegroundColor Green

