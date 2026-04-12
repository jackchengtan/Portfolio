<#
.SYNOPSIS
Moves a processed healthcare ETL CSV file from an inbound folder to an archive folder.

.DESCRIPTION
This script is intended to run after a successful SQL Server ETL load.
It can:
  1. Move the processed CSV into a dated archive folder.
  2. Optionally compress the archived file into a ZIP.
  3. Write a simple execution log.

.NOTES
- Update the folder paths before using.
- This script assumes the ETL has already succeeded.
- You can call it from SQL Agent PowerShell job step or a separate scheduled task.
#>

param(
    [string]$InboundFolder = "C:\ETL\Inbound",
    [string]$ArchiveFolder = "C:\ETL\Archive",
    [string]$LogFolder = "C:\ETL\Logs",
    [string]$FileName,
    [switch]$ZipAfterMove
)

$ErrorActionPreference = "Stop"

function Write-Log {
    param(
        [string]$Message,
        [string]$Level = "INFO"
    )

    if (!(Test-Path $LogFolder)) {
        New-Item -Path $LogFolder -ItemType Directory -Force | Out-Null
    }

    $logFile = Join-Path $LogFolder ("archive_after_load_{0}.log" -f (Get-Date -Format "yyyyMMdd"))
    $line = "[{0}] [{1}] {2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Level, $Message
    Add-Content -Path $logFile -Value $line
    Write-Host $line
}

try {
    if ([string]::IsNullOrWhiteSpace($FileName)) {
        $today = Get-Date -Format "yyyyMMdd"
        $FileName = "patient_encounter_$today.csv"
    }

    $sourcePath = Join-Path $InboundFolder $FileName

    if (!(Test-Path $sourcePath)) {
        throw "Source file not found: $sourcePath"
    }

    $archiveSubFolder = Join-Path $ArchiveFolder (Get-Date -Format "yyyyMMdd")
    if (!(Test-Path $archiveSubFolder)) {
        New-Item -Path $archiveSubFolder -ItemType Directory -Force | Out-Null
    }

    $destinationPath = Join-Path $archiveSubFolder $FileName

    Move-Item -Path $sourcePath -Destination $destinationPath -Force
    Write-Log "Moved file from '$sourcePath' to '$destinationPath'."

    if ($ZipAfterMove) {
        $zipPath = "$destinationPath.zip"

        if (Test-Path $zipPath) {
            Remove-Item -Path $zipPath -Force
        }

        Compress-Archive -Path $destinationPath -DestinationPath $zipPath -Force
        Remove-Item -Path $destinationPath -Force

        Write-Log "Compressed archived file to '$zipPath' and removed the original CSV."
    }

    Write-Log "Archive process completed successfully."
}
catch {
    Write-Log "Archive process failed: $($_.Exception.Message)" "ERROR"
    throw
}
