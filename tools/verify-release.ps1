param(
    [string] $GoRoot = $env:ZMUX_GO_ROOT,
    [string] $SpecRoot = $env:ZMUX_SPEC_ROOT,
    [switch] $RunInterop,
    [switch] $RunBenchmarks,
    [switch] $SkipReleaseProfile
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

function Invoke-Native
{
    param(
        [string] $Command,
        [string[]] $Arguments
    )
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0)
    {
        throw "$Command failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Python
{
    param(
        [string[]] $Arguments
    )
    if (Get-Command "py" -ErrorAction SilentlyContinue)
    {
        Invoke-Native "py" (@("-3") + $Arguments)
        return
    }
    if (Get-Command "python" -ErrorAction SilentlyContinue)
    {
        Invoke-Native "python" $Arguments
        return
    }
    if (Get-Command "python3" -ErrorAction SilentlyContinue)
    {
        Invoke-Native "python3" $Arguments
        return
    }
    throw "python3, python, or py was not found on PATH"
}

function Invoke-Step
{
    param(
        [string] $Name,
        [scriptblock] $Body
    )
    Write-Host ""
    Write-Host "==> $Name"
    & $Body
}

function Invoke-InDirectory
{
    param(
        [string] $Path,
        [scriptblock] $Body
    )
    Push-Location $Path
    try
    {
        & $Body
    }
    finally
    {
        Pop-Location
    }
}

Invoke-InDirectory $repoRoot {
    Invoke-Step "Java clean compile" {
        Invoke-Native "mvn" @("-B", "clean", "compile")
    }

    Invoke-Step "Java reactor tests" {
        Invoke-Native "mvn" @("-B", "test")
    }

    if (-not [string]::IsNullOrWhiteSpace($GoRoot))
    {
        $resolvedGoRoot = (Resolve-Path $GoRoot).Path
        Invoke-Step "Go core tests" {
            Invoke-InDirectory $resolvedGoRoot {
                Invoke-Native "go" @("test", "./...")
            }
        }
        $goAdapterRoot = Join-Path $resolvedGoRoot "adapter/quicmux"
        if (Test-Path $goAdapterRoot)
        {
            Invoke-Step "Go QUIC adapter tests" {
                Invoke-InDirectory $goAdapterRoot {
                    Invoke-Native "go" @("test", "./...")
                }
            }
        }
    }
    else
    {
        Write-Host "Skipping Go tests because -GoRoot / ZMUX_GO_ROOT was not provided."
    }

    if (-not [string]::IsNullOrWhiteSpace($SpecRoot))
    {
        $resolvedSpecRoot = (Resolve-Path $SpecRoot).Path
        $assetValidator = Join-Path $resolvedSpecRoot "tools/validate_assets.py"
        if (Test-Path $assetValidator)
        {
            Invoke-Step "Spec asset validation" {
                Invoke-InDirectory $resolvedSpecRoot {
                    Invoke-Python @("tools/validate_assets.py")
                }
            }
        }
        else
        {
            Write-Host "Skipping spec asset validation because tools/validate_assets.py was not found under $resolvedSpecRoot."
        }
    }
    else
    {
        Write-Host "Skipping spec asset validation because -SpecRoot / ZMUX_SPEC_ROOT was not provided."
    }

    if ($RunInterop)
    {
        if ( [string]::IsNullOrWhiteSpace($GoRoot))
        {
            throw "RunInterop requires -GoRoot or ZMUX_GO_ROOT"
        }
        Invoke-Step "Java core <-> Go core smoke" {
            $previousInterop = $env:ZMUX_INTEROP
            $previousGoRoot = $env:ZMUX_GO_ROOT
            try
            {
                $env:ZMUX_INTEROP = "1"
                $env:ZMUX_GO_ROOT = (Resolve-Path $GoRoot).Path
                Invoke-Native "mvn" @("-B", "-pl", "zmux", "clean", "test", "-Dtest=GoInteropSmokeTest")
            }
            finally
            {
                $env:ZMUX_INTEROP = $previousInterop
                $env:ZMUX_GO_ROOT = $previousGoRoot
            }
        }
    }

    if ($RunBenchmarks)
    {
        Invoke-Step "JMH quick benchmarks" {
            & (Join-Path $repoRoot "tools/run-benchmarks.ps1") -Quick
        }
    }

    if (-not $SkipReleaseProfile)
    {
        Invoke-Step "Release profile verify" {
            Invoke-Native "mvn" @("-B", "-Prelease", "-Dgpg.skip=true", "-DskipTests", "verify")
        }
    }
}
