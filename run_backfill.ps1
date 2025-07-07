
<#
.SYNOPSIS
    TAO-DETECTOR Backfill Manager for Windows
    
.DESCRIPTION
    Simple PowerShell script to manage the backfill process for both exchanges
    
.PARAMETER Action
    The action to perform: check, verify, backfill, binance, okx, status
    
.EXAMPLE
    .\run_backfill.ps1 verify
    .\run_backfill.ps1 backfill
#>

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("check", "verify", "backfill", "binance", "okx", "status")]
    [string]$Action
)

# Colors for output
$Red = [System.ConsoleColor]::Red
$Green = [System.ConsoleColor]::Green
$Yellow = [System.ConsoleColor]::Yellow
$Blue = [System.ConsoleColor]::Blue
$White = [System.ConsoleColor]::White

function Write-ColorOutput {
    param(
        [string]$Message,
        [System.ConsoleColor]$ForegroundColor = $White
    )
    Write-Host $Message -ForegroundColor $ForegroundColor
}

function Test-PythonInstallation {
    try {
        $pythonVersion = python --version 2>$null
        if ($LASTEXITCODE -eq 0) {
            Write-ColorOutput "✅ Python is installed: $pythonVersion" $Green
            return $true
        }
    }
    catch {}
    
    Write-ColorOutput "❌ Python is not installed or not in PATH" $Red
    Write-ColorOutput "Please install Python from https://python.org" $Yellow
    return $false
}

function Test-Dependencies {
    Write-ColorOutput "🔍 Checking Python dependencies..." $Blue
    
    $requiredPackages = @("duckdb", "pandas", "aiohttp", "requests", "pyyaml")
    $missingPackages = @()
    
    foreach ($package in $requiredPackages) {
        try {
            python -c "import $package" 2>$null
            if ($LASTEXITCODE -eq 0) {
                Write-ColorOutput "  ✅ $package" $Green
            } else {
                Write-ColorOutput "  ❌ $package" $Red
                $missingPackages += $package
            }
        }
        catch {
            Write-ColorOutput "  ❌ $package" $Red
            $missingPackages += $package
        }
    }
    
    if ($missingPackages.Count -gt 0) {
        Write-ColorOutput "`nMissing packages detected. Installing..." $Yellow
        $packagesString = $missingPackages -join " "
        Write-ColorOutput "Running: pip install $packagesString" $Blue
        
        try {
            pip install $packagesString
            if ($LASTEXITCODE -eq 0) {
                Write-ColorOutput "✅ Dependencies installed successfully" $Green
                return $true
            } else {
                Write-ColorOutput "❌ Failed to install dependencies" $Red
                return $false
            }
        }
        catch {
            Write-ColorOutput "❌ Failed to install dependencies: $_" $Red
            return $false
        }
    }
    
    Write-ColorOutput "✅ All dependencies are installed" $Green
    return $true
}

function Invoke-BackfillManager {
    param([string]$Action)
    
    Write-ColorOutput "`n🚀 Running: python backfill_manager.py $Action" $Blue
    
    try {
        python backfill_manager.py $Action
        $exitCode = $LASTEXITCODE
        
        switch ($exitCode) {
            0 { Write-ColorOutput "`n✅ Operation completed successfully" $Green }
            1 { Write-ColorOutput "`n⚠️  Operation completed with warnings" $Yellow }
            2 { Write-ColorOutput "`n❌ Operation completed with errors" $Red }
            default { Write-ColorOutput "`n🚨 Operation failed with exit code: $exitCode" $Red }
        }
        
        return $exitCode
    }
    catch {
        Write-ColorOutput "`n🚨 Failed to run backfill manager: $_" $Red
        return 1
    }
}

function Show-Usage {
    Write-ColorOutput "`n🎯 TAO-DETECTOR Backfill Manager" $Blue
    Write-ColorOutput "=================================" $Blue
    Write-ColorOutput "`nUsage: .\run_backfill.ps1 <action>" $White
    Write-ColorOutput "`nAvailable actions:" $White
    Write-ColorOutput "  check     - Check if dependencies are installed" $Green
    Write-ColorOutput "  verify    - Verify current backfill status" $Green
    Write-ColorOutput "  backfill  - Run full backfill for both exchanges" $Green
    Write-ColorOutput "  binance   - Run backfill for Binance only" $Green
    Write-ColorOutput "  okx       - Run backfill for OKX only" $Green
    Write-ColorOutput "  status    - Show detailed database status" $Green
    Write-ColorOutput "`nExamples:" $White
    Write-ColorOutput "  .\run_backfill.ps1 verify" $Yellow
    Write-ColorOutput "  .\run_backfill.ps1 backfill" $Yellow
}

# Main execution
Write-ColorOutput "🎯 TAO-DETECTOR Backfill Manager (Windows)" $Blue
Write-ColorOutput "==========================================" $Blue

# Check if Python is installed
if (-not (Test-PythonInstallation)) {
    exit 1
}

# For most actions, check dependencies
if ($Action -in @("verify", "backfill", "binance", "okx")) {
    if (-not (Test-Dependencies)) {
        Write-ColorOutput "`n❌ Cannot proceed without required dependencies" $Red
        exit 1
    }
}

# Execute the requested action
switch ($Action) {
    "check" {
        Test-Dependencies
    }
    default {
        $exitCode = Invoke-BackfillManager -Action $Action
        
        if ($Action -eq "backfill" -and $exitCode -eq 0) {
            Write-ColorOutput "`n📊 Running verification after backfill..." $Blue
            Invoke-BackfillManager -Action "verify"
        }
        
        exit $exitCode
    }
}
