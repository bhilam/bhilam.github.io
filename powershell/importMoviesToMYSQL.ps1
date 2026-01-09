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
# ============== MySQL Availability Check ===========
# =====================================================
function Test-MySqlServer {
    param(
        [string]$Exe,
        [string]$MySqlHost,
        [string]$User,
        [string]$Pass
    )
    try {
        $cmd = "$Exe -h $MySqlHost -u $User"
        if ($Pass) { $cmd += " -p$Pass" }
        $cmd += " -e `"SELECT 1;`""

        $proc = Start-Process -FilePath "cmd.exe" -ArgumentList "/c $cmd" -NoNewWindow -Wait -PassThru `
            -RedirectStandardError "$env:TEMP\mysql_test_err.txt" `
            -RedirectStandardOutput "$env:TEMP\mysql_test_out.txt"

        if ($proc.ExitCode -ne 0) {
            $err = Get-Content "$env:TEMP\mysql_test_err.txt"
            throw "MySQL server not reachable: $err"
        }
        return $true
    } catch {
        Write-Host "Unable to connect to MySQL server: $_" -ForegroundColor Red
        return $false
    }
}

function Test-MySqlDatabase {
    param(
        [string]$Exe,
        [string]$MySqlHost,
        [string]$User,
        [string]$Pass,
        [string]$DB
    )
    try {
        $cmd = "$Exe -h $MySqlHost -u $User"
        if ($Pass) { $cmd += " -p$Pass" }
        $cmd += " -e `"USE $DB;`""

        $proc = Start-Process -FilePath "cmd.exe" -ArgumentList "/c $cmd" -NoNewWindow -Wait -PassThru `
            -RedirectStandardError "$env:TEMP\mysql_db_err.txt" `
            -RedirectStandardOutput "$env:TEMP\mysql_db_out.txt"

        return ($proc.ExitCode -eq 0)
    } catch {
        return $false
    }
}

# =====================================================
# ================= MySQL Checks =====================
# =====================================================
if (-not $Config.DryRun -and -not (Test-MySqlServer -Exe $Config.MySqlExe -MySqlHost $Config.MySqlHost -User $Config.MySqlUser -Pass $Config.MySqlPass)) {
    Write-Host "Exiting script because MySQL server is not reachable." -ForegroundColor Red
    exit
}

# Create database if it doesn't exist
if (-not $Config.DryRun -and -not (Test-MySqlDatabase -Exe $Config.MySqlExe -MySqlHost $Config.MySqlHost -User $Config.MySqlUser -Pass $Config.MySqlPass -DB $Config.MySqlDB)) {
    Write-Host "Database '$($Config.MySqlDB)' not found. Creating..." -ForegroundColor Yellow
    $createCmd = "$($Config.MySqlExe) -h $($Config.MySqlHost) -u $($Config.MySqlUser)"
    if ($Config.MySqlPass) { $createCmd += " -p$($Config.MySqlPass)" }
    $createCmd += " -e `"CREATE DATABASE $($Config.MySqlDB) CHARACTER SET utf8mb4;`""
    cmd.exe /c $createCmd
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to create database '$($Config.MySqlDB)'." -ForegroundColor Red
        exit
    }
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

    if ($Config.DropTable) {
        $dropCmd = "$($Config.MySqlExe) -h $($Config.MySqlHost) -u $($Config.MySqlUser)"
        if ($Config.MySqlPass) { $dropCmd += " -p$($Config.MySqlPass)" }
        $dropCmd += " $($Config.MySqlDB) -e `"DROP TABLE IF EXISTS movies;`""
        cmd.exe /c $dropCmd
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

    $createTableCmd = "$($Config.MySqlExe) -h $($Config.MySqlHost) -u $($Config.MySqlUser)"
    if ($Config.MySqlPass) { $createTableCmd += " -p$($Config.MySqlPass)" }
    $createTableCmd += " $($Config.MySqlDB) -e `"CREATE TABLE IF NOT EXISTS movies ($($defs -join ',')) ENGINE=InnoDB;`""
    cmd.exe /c $createTableCmd
}

# =====================================================
# ================= MySQL Connection =================
# =====================================================
if (-not $Config.DryRun) {
    Add-Type -Path "C:\Program Files (x86)\MySQL\MySQL Connector NET 9.5\MySql.Data.dll"
    $conn = New-Object MySql.Data.MySqlClient.MySqlConnection(
        "server=$($Config.MySqlHost);uid=$($Config.MySqlUser);database=$($Config.MySqlDB);charset=utf8mb4;pwd=$($Config.MySqlPass)"
    )
    try {
        $conn.Open()
    } catch {
        Write-Host "Failed to open MySQL connection: $_" -ForegroundColor Red
        exit
    }
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

        $percentFile = [math]::Round(($lineNo / $totalLines) * 100, 0)
        Write-Progress -Activity "Processing file $($file.Name)" `
                       -Status "$lineNo / $totalLines lines" `
                       -PercentComplete $percentFile `
                       -Id 1
    }

    InsertBatch $batch
    $reader.Close()

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

