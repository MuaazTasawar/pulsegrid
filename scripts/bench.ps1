<#
.SYNOPSIS
  Runs the delivery benchmark: two CPU-limited workers in Docker, one
  coordinator, and the loadgen binary on this machine.

.DESCRIPTION
  Starts docker-compose.scale.yml (coordinator + worker-a + worker-b on
  disjoint shard prefixes), waits until everything is ready, runs loadgen in
  release mode, prints the result and saves it as JSON under bench-results\.

  Stop natively running coordinator/worker processes first (ports 8090, 8081
  and the shard leases would clash). Numbers are only valid for THIS machine:
  loadgen, Docker, the databases and your desktop all share its CPUs.

.EXAMPLE
  .\scripts\bench.ps1
  .\scripts\bench.ps1 -Devices 1000 -Alerts 50 -IntervalMs 300
  .\scripts\bench.ps1 -UseRunningStack        # stack already up, just run loadgen
#>
[CmdletBinding()]
param(
    [int]$Devices = 500,
    [int]$Alerts = 30,
    [int]$WarmupAlerts = 3,
    [int]$IntervalMs = 500,
    [double]$SpreadMeters = 3000,
    [double]$RadiusMeters = 5000,
    # The default centre sits on the border between shard tkrt (worker A) and
    # tkrs (worker B), so about half of the devices go to each worker.
    [double]$Lat = 24.7852,
    [double]$Lon = 67.0011,
    [string]$Coordinator = "http://localhost:8090",
    [switch]$UseRunningStack,
    [switch]$NoBuild,
    [switch]$KeepUp
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

function Step($text) { Write-Host "-> $text" -ForegroundColor Cyan }
function Fail($text) { Write-Host "FAILED: $text" -ForegroundColor Red; exit 1 }

function Wait-Ready($url, $seconds) {
    $deadline = (Get-Date).AddSeconds($seconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-WebRequest -UseBasicParsing -Uri $url -TimeoutSec 3
            if ($r.StatusCode -eq 200) { return }
        } catch { }
        Start-Sleep -Seconds 2
    }
    Fail "$url did not become ready within $seconds s"
}

$compose = @('-f', 'docker-compose.yml', '-f', 'docker-compose.scale.yml')
$stackServices = @('postgres', 'redis', 'nats', 'api-coordinator', 'worker-a', 'worker-b')

if (-not $UseRunningStack) {
    Step "starting the scale stack (coordinator + 2 workers, CPU-limited)"
    $up = $compose + @('up', '-d')
    if (-not $NoBuild) { $up += '--build' }
    $up += $stackServices
    & docker compose @up
    if ($LASTEXITCODE -ne 0) { Fail "docker compose up failed (if the image build failed, the Dockerfile is the cause)" }
}

Step "waiting for the coordinator and both workers"
Wait-Ready "$Coordinator/ready" 180
Wait-Ready "http://localhost:8081/ready" 180
Wait-Ready "http://localhost:8082/ready" 180

Step "building loadgen (release)"
& cargo build --release -p loadgen
if ($LASTEXITCODE -ne 0) { Fail "cargo build failed" }

Step "running loadgen: $Devices devices, $Alerts measured alerts every $IntervalMs ms"
$lines = & (Join-Path $root 'target\release\loadgen.exe') `
    --coordinator $Coordinator `
    --devices $Devices `
    --alerts $Alerts `
    --warmup-alerts $WarmupAlerts `
    --interval-ms $IntervalMs `
    --lat $Lat `
    --lon $Lon `
    --spread-meters $SpreadMeters `
    --radius-meters $RadiusMeters `
    --json
$loadgenExit = $LASTEXITCODE
$text = ($lines -join "`n").Trim()
if (-not $text) { Fail "loadgen printed no result (exit code $loadgenExit)" }
$result = $text | ConvertFrom-Json

$cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
$ramGb = [math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 1)
$machine = [ordered]@{
    cpu              = $cpu.Name.Trim()
    cores            = $cpu.NumberOfCores
    logical_threads  = $cpu.NumberOfLogicalProcessors
    ram_gb           = $ramGb
    note             = 'loadgen, Docker, databases and the desktop share these CPUs; workers are limited to 1 CPU each'
}

$delivery = $result.delivery_latency_ms
$post = $result.post_alerts_ms
Write-Host ""
Write-Host "=== Result ===" -ForegroundColor Green
Write-Host ("devices:     {0} connected, {1} failed" -f $result.devices_connected, $result.devices_failed)
$result.devices_per_worker.PSObject.Properties | ForEach-Object { Write-Host ("             {0}: {1}" -f $_.Name, $_.Value) }
Write-Host ("deliveries:  {0} of {1} ({2:P2}), duplicates {3}" -f $result.unique_deliveries, $result.expected_deliveries, $result.delivery_ratio, $result.duplicate_deliveries)
Write-Host ("rate:        {0:N0} deliveries/s over {1:N1} s (offered load, not a ceiling)" -f $result.deliveries_per_sec, $result.window_secs)
Write-Host ("delivery ms: p50 {0:N2}  p90 {1:N2}  p99 {2:N2}  p99.9 {3:N2}  max {4:N2}" -f $delivery.p50, $delivery.p90, $delivery.p99, $delivery.p99_9, $delivery.max)
Write-Host ("POST ms:     p50 {0:N2}  p90 {1:N2}  p99 {2:N2}" -f $post.p50, $post.p90, $post.p99)

$outDir = Join-Path $root 'bench-results'
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$file = Join-Path $outDir ("bench-{0}.json" -f (Get-Date -Format 'yyyyMMdd-HHmmss'))
$saved = [ordered]@{
    taken_at = (Get-Date).ToString('o')
    machine  = $machine
    result   = $result
}
$json = $saved | ConvertTo-Json -Depth 8
[System.IO.File]::WriteAllText($file, $json, (New-Object System.Text.UTF8Encoding($false)))
Write-Host "saved $file"

if (-not $UseRunningStack -and -not $KeepUp) {
    Step "stopping the coordinator and workers (postgres, redis, nats stay up)"
    & docker compose @compose stop api-coordinator worker-a worker-b
}

if ($loadgenExit -ne 0) { Fail "loadgen exited with code $loadgenExit" }
exit 0