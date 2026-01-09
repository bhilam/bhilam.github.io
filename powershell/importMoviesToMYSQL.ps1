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

# Clear duplicates log at start
if (Test-Path $Config.DuplicateFile) { Remove-Item $Config.DuplicateFile }

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
    $s = "$($v.FORMATTEDTITLE.Trim())|$($v.YEAR.Trim())|$($v.DIRECTOR.Trim())|$($v.URL.Trim())|$($v.FILEPATH.Trim())"
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
        $row[$Columns[$i]] = if ($v -ne $null) { $v.Trim() } else { $null }
    }
    return $row
}

# =====================================================
# ================= MySQL Setup =====================
# =====================================================
Add-Type -Path "C:\Program Files (x86)\MySQL\MySQL Connector NET 9.5\MySql.Data.dll"
$conn = New-Object MySql.Data.MySqlClient.MySqlConnection(
    "server=$($Config.MySqlHost);uid=$($Config.MySqlUser);database=$($Config.MySqlDB);charset=utf8mb4;pwd=$($Config.MySqlPass)"
)
$conn.Open()
$tx = $conn.BeginTransaction()

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
# ================= Database Setup ===================
# =====================================================
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

# =====================================================
# ================= Batch Insert =====================
# =====================================================
function InsertBatch($rows, $currentFileName) {
    if (-not $rows -or $Config.DryRun) { return }

    $cols = $Columns + "RECORDHASH"

    # Compute hashes
    foreach ($row in $rows) {
        $row.RECORDHASH = Get-RecordHash $row
    }

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
    $cmd.CommandText = "INSERT IGNORE INTO movies ($columnList) VALUES " + ($valuesList -join ',')

    $affected = $cmd.ExecuteNonQuery()
    $global:committedRecords += $affected

    # Log duplicates with key fields and file name
    $duplicatesCount = $rows.Count - $affected
    if ($duplicatesCount -gt 0) {
        $hashes = $rows | ForEach-Object { $_.RECORDHASH }
        $hashParam = "'" + ($hashes -join "','") + "'"
        $checkCmd = $conn.CreateCommand()
        $checkCmd.CommandText = "SELECT RECORDHASH, FORMATTEDTITLE, YEAR, DIRECTOR, URL, FILEPATH FROM movies WHERE RECORDHASH IN ($hashParam)"
        $reader = $checkCmd.ExecuteReader()
        while ($reader.Read()) {
            $logLine = "File:$currentFileName | Hash:$($reader['RECORDHASH']) | Title:$($reader['FORMATTEDTITLE']) | Year:$($reader['YEAR']) | Director:$($reader['DIRECTOR']) | URL:$($reader['URL']) | FilePath:$($reader['FILEPATH'])"
            $logLine | Out-File $Config.DuplicateFile -Append
        }
        $reader.Close()
    }
}

# =====================================================
# ================= Import Loop ======================
# =====================================================
$totalRecords = 0
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
        $batch += $row

        if ($batch.Count -ge $Config.BatchSize) {
            InsertBatch $batch $file.Name
            $batch = @()
        }

        $percentFile = [math]::Round(($lineNo / $totalLines) * 100, 0)
        Write-Progress -Activity "Processing file $($file.Name)" `
                       -Status "$lineNo / $totalLines lines" `
                       -PercentComplete $percentFile `
                       -Id 1
    }

    InsertBatch $batch $file.Name
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
$duplicates = (Get-Content $Config.DuplicateFile -ErrorAction SilentlyContinue | Measure-Object | Select-Object -ExpandProperty Count)

$summary = @"
============== Import completed $(Get-Date) ==============
Files processed          : $currentFile
Total records parsed     : $totalRecords
Records committed        : $committedRecords
Duplicates logged        : $duplicates
Duplicate log file       : $($Config.DuplicateFile)
Main log file            : $($Config.LogFile)
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
