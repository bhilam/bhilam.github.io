Set-PSDebug -Trace 0;

$fileExtFilter = "*.sql"
$sourceSQLFileLocation = "C:\wamp64\www\movies\antexport - copy\"
$destinationSQLFileLocation  = "C:\wamp64\www\movies\antexport\"
$destinationSQLFIleName = "movies.sql"
$finalSQLFilePathAndName = $destinationSQLFileLocation + $destinationSQLFIleName

if (Test-Path "$sourceSQLFileLocation") 
{
	$fileCount = [System.IO.Directory]::GetFiles($sourceSQLFileLocation, $fileExtFilter).Count
	Write-Host $fileCount "- Files with SQL extention found at" $sourceSQLFileLocation
	if ($fileCount -gt 0)
	{
		Get-ChildItem -Path $sourceSQLFileLocation -Filter $fileExtFilter | Sort-Object { [regex]::Replace($_.Name, '\d+', { $args[0].Value.PadLeft(20) }) }
	} else {
		Write-Host $sourceSQLFileLocation " - There are no SQL File at that source location."
	}
} else {
    Write-Host $sourceSQLFileLocation " - No such location exist."
	exit;
}


if (Test-Path $finalSQLFilePathAndName) 
{
  Remove-Item $finalSQLFilePathAndName -Force
    Write-Host $finalSQLFilePathAndName " - File deleted."
} else {
    Write-Host $finalSQLFilePathAndName " - File does not exist."
}

Get-ChildItem $sourceSQLFileLocation -Filter *.sql | Sort-Object { [regex]::Replace($_.Name, '\d+', { $args[0].Value.PadLeft(20) }) } | % {
    $file = $_.Name
    " " | Out-File -Append $finalSQLFilePathAndName
    "-----------------------------------" | Out-File -Append $finalSQLFilePathAndName
    "--${file}:" | Out-File -Append $finalSQLFilePathAndName
           " " | Out-File -Append $finalSQLFilePathAndName
    Get-Content $_.FullName | % {
        "$_" | Out-File -Append $finalSQLFilePathAndName
    }
	if (Test-Path $finalSQLFilePathAndName) {
		Write-Host "Appended File:" $_.FullName
	} else {
		Write-Host $_.FullName " was NOT appended. Please revisit the directory/script."
	}
}
Write-Host "Done!!!"
