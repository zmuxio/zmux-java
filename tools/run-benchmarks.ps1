param(
    [string] $Includes = "io.zmux.benchmarks.CodecBenchmark|io.zmux.internal.OrdinaryBatchOrdererBenchmark|io.zmux.internal.FlowControlRegistryBenchmark",
    [switch] $Quick,
    [string] $ResultFile = "zmux-benchmarks/target/jmh-result.json"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

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

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Push-Location $repoRoot
try
{
    Invoke-Native "mvn" @("-B", "-Pbenchmarks", "-pl", "zmux-benchmarks", "-am", "-DskipTests", "package")

    $jar = Join-Path $repoRoot "zmux-benchmarks/target/benchmarks.jar"
    if (-not (Test-Path $jar))
    {
        throw "Benchmark jar was not generated: $jar"
    }

    $jmhArgs = @(
        "-jar", $jar,
        $Includes,
        "-rf", "json",
        "-rff", $ResultFile
    )
    if ($Quick)
    {
        $jmhArgs += @("-wi", "1", "-i", "2", "-w", "500ms", "-r", "500ms", "-f", "1")
    }

    Invoke-Native "java" $jmhArgs
}
finally
{
    Pop-Location
}
