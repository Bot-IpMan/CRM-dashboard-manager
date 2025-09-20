[CmdletBinding()]
param(
    [string]$ConfigPath,
    [switch]$Once,
    [switch]$SkipInstall
)

$ErrorActionPreference = 'Stop'
$scriptDirectory = Split-Path -LiteralPath $MyInvocation.MyCommand.Path -Parent
$repositoryRoot = Split-Path -LiteralPath $scriptDirectory -Parent
Push-Location $repositoryRoot
try {
    if (-not $PSBoundParameters.ContainsKey('ConfigPath')) {
        $ConfigPath = Join-Path $repositoryRoot 'config.windows.json'
        if (-not (Test-Path -LiteralPath $ConfigPath) -and (Test-Path -LiteralPath (Join-Path $repositoryRoot 'config.example.json'))) {
            Write-Host "Creating default Windows configuration at $ConfigPath" -ForegroundColor Cyan
            Copy-Item -LiteralPath (Join-Path $repositoryRoot 'config.example.json') -Destination $ConfigPath
            Write-Host 'Please review and adjust the paths in the generated configuration before running the service again.' -ForegroundColor Yellow
            return
        }
    }

    if (-not [System.IO.Path]::IsPathRooted($ConfigPath)) {
        $ConfigPath = Join-Path (Get-Location) $ConfigPath
    }

    if (-not (Test-Path -LiteralPath $ConfigPath)) {
        throw "Configuration file '$ConfigPath' was not found. Provide the path via -ConfigPath."
    }

    function Invoke-CheckedCommand {
        param(
            [string]$FilePath,
            [string[]]$Arguments
        )

        & $FilePath @Arguments
        if ($LASTEXITCODE -ne 0) {
            throw "Command '$FilePath' failed with exit code $LASTEXITCODE."
        }
    }

    $launcher = Get-Command py.exe -ErrorAction SilentlyContinue
    if (-not $launcher) {
        $launcher = Get-Command python.exe -ErrorAction SilentlyContinue
    }
    if (-not $launcher) {
        throw 'Python 3.10 or newer is required. Install it and ensure "py" or "python" is available in PATH.'
    }

    $venvPath = Join-Path $repositoryRoot '.venv'
    $pythonExe = Join-Path $venvPath 'Scripts/python.exe'

    if (-not (Test-Path -LiteralPath $pythonExe)) {
        Write-Host "Creating virtual environment in $venvPath" -ForegroundColor Cyan
        $venvArgs = @('-m', 'venv', $venvPath)
        if ($launcher.Name -ieq 'py.exe') {
            $venvArgs = @('-3') + $venvArgs
        }
        Invoke-CheckedCommand -FilePath $launcher.Source -Arguments $venvArgs
    }

    if (-not (Test-Path -LiteralPath $pythonExe)) {
        throw "Unable to locate virtual environment Python interpreter at $pythonExe."
    }

    if (-not $SkipInstall.IsPresent) {
        Write-Host 'Installing/updating dependencies...' -ForegroundColor Cyan
        Invoke-CheckedCommand -FilePath $pythonExe -Arguments @('-m', 'pip', 'install', '--upgrade', 'pip')
        Invoke-CheckedCommand -FilePath $pythonExe -Arguments @('-m', 'pip', 'install', '-r', 'requirements.txt')
    }

    $arguments = @('-m', 'crm_file_event_service', '--config', $ConfigPath)
    if ($Once.IsPresent) {
        $arguments += '--once'
    }

    Write-Host "Starting file event service with configuration $ConfigPath" -ForegroundColor Green
    & $pythonExe @arguments
}
finally {
    Pop-Location
}
