# =====================================================
# ================= Configuration ====================
# =====================================================

param(
    [string]$SourceFolder   = "C:\wamp64-3.3.7\www\movies\antexport - copy",
    [string]$MySqlExe       = "C:\wamp64-3.3.7\bin\mysql\mysql9.1.0\bin\mysql.exe",
    [string]$MySqlHost      = "localhost",
    [string]$MySqlUser      = "root",
    [string]$MySqlPass      = "",
    [string]$MySqlDB        = "movies",
    [string]$TableName      = "movies",
    [string]$LogFile        = "import_log.txt",
    [string]$DuplicateFile  = "duplicates_log.txt",
    [bool]$DryRun           = $false,
    [bool]$DropTable        = $true,
    [string]$SqlFileFilter  = "movies_*_*-*.sql",
    [string[]]$ExcludeFiles = @('movies_00_0000.sql'),
    [int]$BatchSize         = 500
)

# =====================================================
# =============== Encoding Setup =====================
# =====================================================

$InputEncoding  = [Text.Encoding]::GetEncoding("ISO-8859-1")
$OutputEncoding = [Text.Encoding]::UTF8

$Config = @{
    SourceFolder   = $SourceFolder
    MySqlExe       = $MySqlExe
    MySqlHost      = $MySqlHost
    MySqlUser      = $MySqlUser
    MySqlPass      = $MySqlPass
    MySqlDB        = $MySqlDB
    TableName      = $TableName
    LogFile        = Join-Path $SourceFolder $LogFile
    DuplicateFile  = Join-Path $SourceFolder $DuplicateFile
    DryRun         = $DryRun
    DropTable      = $DropTable
    InputEncoding  = $InputEncoding
    OutputEncoding = $OutputEncoding
    SqlFileFilter  = $SqlFileFilter
    ExcludeFiles   = $ExcludeFiles
    BatchSize      = $BatchSize
}

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
    ([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($s)))) -replace '-', ''
}

function ParseInsertLine {
    param ($line, $Columns)

    if ($line -notmatch "^INSERT INTO .*?\s*\(.+?\)\s*VALUES\s*\(.+\);$") { return $null }

    $vals = [regex]::Split(
        ($line -replace '^.+?VALUES\s*\(|\);$',''),
        ",(?=(?:[^']*'[^']*')*[^']*$)"
    )

    if ($vals.Count -ne $Columns.Count) { return $null }

    $row = @{}
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
# ================= MySQL Checks =====================
# =====================================================
function Test-MySqlServer {
    param($Exe, $MySqlHost, $User, $Pass)
    try {
        $cmd = "$Exe -h $MySqlHost -u $User"
        if ($Pass) { $cmd += " -p$Pass" }
        $cmd += ' -e "SELECT 1;"'
        $proc = Start-Process -FilePath "cmd.exe" -ArgumentList "/c $cmd" -NoNewWindow -Wait -PassThru `
            -RedirectStandardError "$env:TEMP\mysql_test_err.txt" `
            -RedirectStandardOutput "$env:TEMP\mysql_test_out.txt"
        return ($proc.ExitCode -eq 0)
    } catch { return $false }
}

function Test-MySqlDatabase {
    param($Exe, $MySqlHost, $User, $Pass, $DB)
    try {
        $cmd = "$Exe -h $MySqlHost -u $User"
        if ($Pass) { $cmd += " -p$Pass" }
        $cmd += " -e `"USE $DB;`""
        $proc = Start-Process -FilePath "cmd.exe" -ArgumentList "/c $cmd" -NoNewWindow -Wait -PassThru `
            -RedirectStandardError "$env:TEMP\mysql_db_err.txt" `
            -RedirectStandardOutput "$env:TEMP\mysql_db_out.txt"
        return ($proc.ExitCode -eq 0)
    } catch { return $false }
}

if (-not $Config.DryRun -and -not (Test-MySqlServer $Config.MySqlExe $Config.MySqlHost $Config.MySqlUser $Config.MySqlPass)) {
    Write-Host "MySQL server not reachable." -ForegroundColor Red
    exit
}

if (-not $Config.DryRun -and -not (Test-MySqlDatabase $Config.MySqlExe $Config.MySqlHost $Config.MySqlUser $Config.MySqlPass $Config.MySqlDB)) {
    Write-Host "Database '$($Config.MySqlDB)' not found. Creating..." -ForegroundColor Yellow
    $createCmd = "$($Config.MySqlExe) -h $($Config.MySqlHost) -u $($Config.MySqlUser)"
    if ($Config.MySqlPass) { $createCmd += " -p$($Config.MySqlPass)" }
    $createCmd += " -e `"CREATE DATABASE $($Config.MySqlDB) CHARACTER SET utf8mb4;`""
    cmd.exe /c $createCmd
    if ($LASTEXITCODE -ne 0) { Write-Host "Failed to create database." -ForegroundColor Red; exit }
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
# =============== Detect Columns ====================
# =====================================================
$firstInsert = Get-Content $sqlFiles[0].FullName -Encoding $Config.InputEncoding |
               Where-Object { $_ -match "INSERT INTO" } |
               Select-Object -First 1

if (-not $firstInsert) {
    Write-Host "No valid INSERT statement found in first SQL file. Cannot detect columns." -ForegroundColor Red
    exit
}

$Columns = ($firstInsert -replace '^.+?\(|\)\s*VALUES.+$','') -split '\s*,\s*'

# =====================================================
# =============== MySQL Connection ===================
# =====================================================
Add-Type -Path "C:\Program Files (x86)\MySQL\MySQL Connector NET 9.5\MySql.Data.dll"
$conn = New-Object MySql.Data.MySqlClient.MySqlConnection(
    "server=$($Config.MySqlHost);uid=$($Config.MySqlUser);database=$($Config.MySqlDB);charset=utf8mb4;pwd=$($Config.MySqlPass)"
)
try { $conn.Open() } catch { Write-Host "MySQL connection failed: $_" -ForegroundColor Red; exit }
$tx = $conn.BeginTransaction()

# =====================================================
# ================= Database Setup ===================
# =====================================================
if (-not $Config.DryRun) {
    if ($Config.DropTable) {
        $dropCmd = $conn.CreateCommand()
        $dropCmd.CommandText = "DROP TABLE IF EXISTS $($Config.TableName);"
        $dropCmd.ExecuteNonQuery() | Out-Null
    }

    $defs = @()
    foreach ($c in $Columns) {
        if ($c -eq "NUM") { $defs += "$c INT NOT NULL PRIMARY KEY" } else { $defs += "$c TEXT CHARACTER SET utf8mb4" }
    }
    $defs += "RECORDHASH CHAR(64) NOT NULL UNIQUE"

    $createCmd = $conn.CreateCommand()
    $createCmd.CommandText = "CREATE TABLE IF NOT EXISTS $($Config.TableName) ($($defs -join ',')) ENGINE=InnoDB;"
    $createCmd.ExecuteNonQuery() | Out-Null
}

# =====================================================
# ================= Duplicate Detection ==============
# =====================================================
$existingHashes = @{}
$checkCmd = $conn.CreateCommand()
$checkCmd.CommandText = "SELECT RECORDHASH FROM $($Config.TableName);"
$reader = $checkCmd.ExecuteReader()
while ($reader.Read()) { $existingHashes[$reader["RECORDHASH"]] = $true }
$reader.Close()

# =====================================================
# ================= Batch Insert =====================
# =====================================================
function InsertBatch($rows) {
    if (-not $rows -or $Config.DryRun) { return }
    $cols = $Columns + "RECORDHASH"
    $cmd = $conn.CreateCommand()
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
    $cmd.CommandText = "INSERT INTO $($Config.TableName) ($columnList) VALUES " + ($valuesList -join ',')
    $global:committedRecords += $cmd.ExecuteNonQuery()
}

# =====================================================
# ================= Import Loop ======================
# =====================================================
$totalRecords = 0
$duplicates   = 0
$currentFile  = 0
$globalStart  = Get-Date
$global:committedRecords = 0

foreach ($file in $sqlFiles) {
    $currentFile++
    $reader = [IO.StreamReader]::new($file.FullName,$Config.InputEncoding)
    $batch  = @()
    $lineNo = 0
    $totalLines = (Get-Content $file.FullName -Encoding $Config.InputEncoding).Count

    while (-not $reader.EndOfStream) {
        $lineNo++
        $row = ParseInsertLine (NormalizeLine $reader.ReadLine()) $Columns
        if (-not $row) { continue }

        $hash = Get-RecordHash $row
        $totalRecords++

        if ($existingHashes.ContainsKey($hash)) {
            $duplicates++
            "File:$($file.Name) Line:$lineNo Hash:$hash" | Out-File $Config.DuplicateFile -Append -Encoding $Config.OutputEncoding
            continue
        }

        $row.RECORDHASH = $hash
        $existingHashes[$hash] = $true
        $batch += $row

        if ($batch.Count -ge $Config.BatchSize) { InsertBatch $batch; $batch=@() }

        $percentFile = [math]::Round(($lineNo/$totalLines)*100,0)
        Write-Progress -Activity "Processing file $($file.Name)" -Status "$lineNo / $totalLines lines" -PercentComplete $percentFile -Id 1
    }

    InsertBatch $batch
    $reader.Close()

    $percentTotal = [math]::Round(($currentFile / $sqlFiles.Count)*100,0)
    Write-Progress -Activity "Overall Progress" -Status "File $currentFile / $($sqlFiles.Count) processed" -PercentComplete $percentTotal -Id 0
}

if (-not $Config.DryRun) { $tx.Commit(); $conn.Close() }

# =====================================================
# ================= Summary ==========================
# =====================================================
$elapsed = Get-ElapsedSeconds $globalStart

$summary = @"
============== Import completed $(Get-Date) ==============
Files processed          : $currentFile
Total records parsed     : $totalRecords
Records committed        : $global:committedRecords
Duplicates logged        : $duplicates
Duplicate log file       : $($Config.DuplicateFile)
Hash fields              : FORMATTEDTITLE | YEAR | DIRECTOR | URL | FILEPATH
Time taken               : {0:N2} sec
Dry-run mode             : $($Config.DryRun)
Drop table before import : $($Config.DropTable)
Input charset            : latin1 (ISO-8859-1)
Output charset           : utf8
SQL file filter          : $($Config.SqlFileFilter)
Excluded files           : $($Config.ExcludeFiles -join ', ')
Database table           : $($Config.TableName)
=========================================================
"@ -f $elapsed

$summary | Out-File $Config.LogFile -Append -Encoding $Config.OutputEncoding
Write-Host $summary -ForegroundColor Green
