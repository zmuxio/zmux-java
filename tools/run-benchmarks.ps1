param([switch] $Quick, [switch] $LoadedOnly, [switch] $FootprintOnly, [string] $ResultFile = "target/benchmark.json")
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
function Invoke-Timed([string] $Command, [int] $Seconds) {
    $process = Start-Process -FilePath $env:ComSpec -ArgumentList @('/d','/c', $Command) -WorkingDirectory $repoRoot -NoNewWindow -PassThru
    $handle = $process.Handle
    if (-not $process.WaitForExit($Seconds * 1000)) {
        taskkill /PID $process.Id /T /F
        throw "Benchmark command timed out"
    }
    if ($process.ExitCode -ne 0) { throw "Benchmark command failed: $($process.ExitCode)" }
}
if ($LoadedOnly -and $FootprintOnly) { throw "Choose one measurement family" }
Invoke-Timed 'mvn -B -pl zmux -am -DskipTests package' 600
$quickArg = if ($Quick) { ' --quick' } else { '' }
if ($LoadedOnly) { $quickArg += ' --loaded-only' }
if ($FootprintOnly) { $quickArg += ' --footprint-only' }
if ($ResultFile.Contains('"')) { throw "Invalid result path" }
Invoke-Timed ('java --class-path zmux/target/classes tools/Benchmark.java' + $quickArg + ' --result-file "' + $ResultFile + '"') 330
