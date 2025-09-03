<# 
 AGV RTLS Dashboard - Installation Script (PowerShell)
 Mirrors install.sh but runs natively on Windows / PowerShell.

 What it does:
 1) Reads .env (MYSQL_* etc.)
 2) Finds Python 3.11+, creates venv, installs requirements
 3) Initializes MySQL schema using MYSQL_ROOT_PASSWORD from .env (or prompts)
 4) Tries to start Mosquitto service on Windows if installed
#>

$ErrorActionPreference = "Stop"

Write-Host "========================================="
Write-Host "AGV RTLS Dashboard - Installation Script"
Write-Host "========================================="

# ------------------------- Helpers -------------------------

function Read-DotEnv {
    param(
        [string]$Path = ".env"
    )
    $envMap = @{}

    if (-not (Test-Path $Path)) {
        Write-Host "No .env found at '$Path'. Skipping env load (defaults will be used)." -ForegroundColor Yellow
        return $envMap
    }

    Get-Content -Raw -Path $Path -ErrorAction Stop |
    ForEach-Object {
        $_ -split "`n"
    } |
    ForEach-Object {
        $line = $_.Trim()
        if ([string]::IsNullOrWhiteSpace($line)) { return }
        if ($line.StartsWith("#")) { return }

        # KEY=VALUE (tolerate spaces around '=')
        if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$') {
            $key = $matches[1].Trim()
            $val = $matches[2]

            # Handle quoted values
            if ($val.StartsWith('"') -and $val.EndsWith('"')) {
                $val = $val.Trim('"')
            } elseif ($val.StartsWith("'") -and $val.EndsWith("'")) {
                $val = $val.Trim("'")
            } else {
                # Strip inline comments like: VALUE  # comment
                $hashIndex = $val.IndexOf(" #")
                if ($hashIndex -ge 0) {
                    $val = $val.Substring(0, $hashIndex)
                }
                $val = $val.Trim()
            }
            $envMap[$key] = $val
        }
    }

    return $envMap
}

function Get-PythonCmd {
    # Returns: @{Exe="py"; Args=@("-3.11")} or falls back to other combos
    $cmd = @{}

    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) {
        # Prefer 3.11
        try {
            & $py.Path -3.11 -V | Out-Null
            $cmd = @{ Exe = $py.Path; Args = @("-3.11") }
            return $cmd
        } catch {}
        try {
            & $py.Path -3 -V | Out-Null
            $cmd = @{ Exe = $py.Path; Args = @("-3") }
            return $cmd
        } catch {}
    }

    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($python) {
        $cmd = @{ Exe = $python.Path; Args = @() }
        return $cmd
    }

    $python3 = Get-Command python3 -ErrorAction SilentlyContinue
    if ($python3) {
        $cmd = @{ Exe = $python3.Path; Args = @() }
        return $cmd
    }

    throw "Python not found. Please install Python 3.11+."
}

function Get-PythonVersionMajorMinor([string]$Exe, [string[]]$Args) {
    $code = 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")'
    $output = & $Exe @Args -c $code
    return $output.Trim()
}

function Assert-VersionGE([string]$actual, [string]$required) {
    $a = $actual.Split('.'); $b = $required.Split('.')
    if ([int]$a[0] -gt [int]$b[0]) { return }
    if ([int]$a[0] -lt [int]$b[0]) { throw "Python $required+ required, found $actual" }
    if ([int]$a[1] -lt [int]$b[1]) { throw "Python $required+ required, found $actual" }
}

function Ensure-MySQL {
    $mysql = Get-Command mysql -ErrorAction SilentlyContinue
    if (-not $mysql) {
        throw "MySQL client 'mysql.exe' not found in PATH. Install MySQL and ensure 'mysql.exe' is available."
    }
    return $mysql.Path
}

function Invoke-MySQLScript {
    param(
        [string]$MySqlExe,
        [string]$Host,
        [int]$Port,
        [string]$RootPassword,
        [string]$SchemaPath
    )
    if (-not (Test-Path $SchemaPath)) {
        throw "Schema file not found: $SchemaPath"
    }
    # Use MYSQL_PWD to avoid password echo in process list
    $old = $env:MYSQL_PWD
    try {
        $env:MYSQL_PWD = $RootPassword

        # Use mysql client 'source' command (works fine on Windows)
        $schema = (Resolve-Path $SchemaPath).Path -replace '\\','/'
        & $MySqlExe -u root -h $Host -P $Port --protocol=tcp -e "source $schema"
    }
    finally {
        if ($null -ne $old) { $env:MYSQL_PWD = $old } else { Remove-Item Env:\MYSQL_PWD -ErrorAction SilentlyContinue }
    }
}

# ------------------------- Load .env -------------------------

$envData = Read-DotEnv ".env"

# Defaults + .env values
$MySQLHost = if ($envData.ContainsKey("MYSQL_HOST")) { $envData["MYSQL_HOST"] } else { "127.0.0.1" }
$MySQLPort = if ($envData.ContainsKey("MYSQL_PORT")) { [int]$envData["MYSQL_PORT"] } else { 3306 }
$MySQLRootPass = if ($envData.ContainsKey("MYSQL_ROOT_PASSWORD")) { $envData["MYSQL_ROOT_PASSWORD"] } else { "" }

# Treat placeholder as empty
if ($MySQLRootPass -eq "your_root_password_here") { $MySQLRootPass = "" }

# ------------------------- Python + venv -------------------------

Write-Host "Detecting Python..."
$pyCmd = Get-PythonCmd
$pyExe = $pyCmd.Exe
$pyArgs = $pyCmd.Args

$pyVer = Get-PythonVersionMajorMinor -Exe $pyExe -Args $pyArgs
Write-Host "Using Python $pyVer via: $pyExe $($pyArgs -join ' ')"
Assert-VersionGE $pyVer "3.11"

Write-Host "Creating virtual environment..."
& $pyExe @pyArgs -m venv venv

$venvPython = Join-Path $PWD "venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    throw "Failed to create venv Python at $venvPython"
}

Write-Host "Upgrading pip..."
& $venvPython -m pip install --upgrade pip --trusted-host pypi.org --trusted-host files.pythonhosted.org

Write-Host "Installing Python packages from requirements.txt..."
& $venvPython -m pip install -r requirements.txt

# ------------------------- MySQL schema -------------------------

$mysqlExe = Ensure-MySQL

if ([string]::IsNullOrWhiteSpace($MySQLRootPass)) {
    # Prompt securely if not set in .env
    $sec = Read-Host -AsSecureString -Prompt "Enter MySQL root password"
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec)
    try {
        $MySQLRootPass = [Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr)
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}

Write-Host "Setting up MySQL database (host=$MySQLHost, port=$MySQLPort)..."
Invoke-MySQLScript -MySqlExe $mysqlExe -Host $MySQLHost -Port $MySQLPort -RootPassword $MySQLRootPass -SchemaPath "database\schema.sql"

# ------------------------- Mosquitto (Windows) -------------------------

try {
    $svc = Get-Service -Name "mosquitto" -ErrorAction Stop
    if ($svc.Status -ne "Running") {
        Write-Host "Starting Mosquitto service..."
        Start-Service -Name "mosquitto"
    }
} catch {
    Write-Host "Mosquitto service not found. If needed, install from https://mosquitto.org/download/" -ForegroundColor Yellow
}

Write-Host "========================================="
Write-Host "Installation complete!"
Write-Host "========================================="
Write-Host ""
Write-Host "Next steps:"
Write-Host "1. Verify/update .env as needed (already loaded by this script)."
Write-Host "2. Place your plant_map.png in assets\"
Write-Host "3. Configure zones in assets\zones.geojson"
Write-Host "4. Run calibration: `venv\Scripts\python.exe scripts\calibrate_transform.py`"
Write-Host "5. Start services (if you have a PS script): .\scripts\start_services.ps1"
Write-Host ""
