<#
.SYNOPSIS
  End-to-end check of a running PulseGrid stack: register a device, get a
  WebSocket ticket, connect, fire an alert, and wait for it to arrive.

.DESCRIPTION
  Needs the coordinator and a fanout-worker that owns the device's shard to be
  running, and OPERATOR_API_KEYS to be set in .env (run scripts\gen-secrets.ps1).
  Exits 0 when the alert arrives, 1 otherwise. Works in Windows PowerShell 5.1
  and PowerShell 7.

.EXAMPLE
  .\scripts\smoke.ps1
  .\scripts\smoke.ps1 -Lat 24.86 -Lon 67.00 -RadiusMeters 3000
#>
[CmdletBinding()]
param(
    [string]$Coordinator = "http://localhost:8090",
    [double]$Lat = 24.8607,
    [double]$Lon = 67.0011,
    [double]$RadiusMeters = 5000,
    [int]$TimeoutSeconds = 10
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot

function Step($text) { Write-Host "-> $text" -ForegroundColor Cyan }
function Fail($text) { Write-Host "FAILED: $text" -ForegroundColor Red; exit 1 }

$envFile = Join-Path $root '.env'
if (-not (Test-Path $envFile)) { Fail ".env not found. Run .\scripts\gen-secrets.ps1 first." }
$match = Select-String -Path $envFile -Pattern '^OPERATOR_API_KEYS=(.+)$' | Select-Object -First 1
if (-not $match) { Fail "OPERATOR_API_KEYS is not set in .env. Run .\scripts\gen-secrets.ps1." }
$operatorKey = ($match.Matches[0].Groups[1].Value -split ',')[0].Trim()

$ws = $null
$deviceAuth = $null
try {
    Step "registering a device at ($Lat, $Lon)"
    $body = @{ lat = $Lat; lon = $Lon } | ConvertTo-Json
    $device = Invoke-RestMethod -Method Post "$Coordinator/devices/register" -ContentType 'application/json' -Body $body
    $deviceAuth = @{ Authorization = "Bearer $($device.token)" }
    Write-Host "   device $($device.device_id) in shard $($device.shard_prefix)"

    Step "asking for a one-time WebSocket ticket"
    $ticket = Invoke-RestMethod -Method Post "$Coordinator/devices/ws-ticket" -Headers $deviceAuth
    if (-not $ticket.ws_url) {
        Fail "no fanout-worker owns shard $($ticket.shard_prefix). Start one with FANOUT_WORKER_SHARD_PREFIXES including it."
    }
    Write-Host "   worker: $($ticket.ws_url) (ticket valid for $($ticket.expires_in_secs) s)"

    Step "opening the WebSocket"
    $uri = [Uri]"$($ticket.ws_url)/connect/$($device.device_id)?ticket=$($ticket.ticket)"
    $cts = New-Object System.Threading.CancellationTokenSource
    $cts.CancelAfter([TimeSpan]::FromSeconds($TimeoutSeconds))
    $ws = New-Object System.Net.WebSockets.ClientWebSocket
    $ws.ConnectAsync($uri, $cts.Token).GetAwaiter().GetResult() | Out-Null
    Write-Host "   connected (state: $($ws.State))"

    Step "firing a $RadiusMeters m alert centred on the device"
    $alertBody = @{
        lat = $Lat; lon = $Lon; radius_meters = $RadiusMeters
        severity = 'warning'; title = 'PulseGrid smoke test'; message = 'If you can read this, fanout works.'
    } | ConvertTo-Json
    $sent = [DateTime]::UtcNow
    $alert = Invoke-RestMethod -Method Post "$Coordinator/alerts" -Headers @{ 'X-API-Key' = $operatorKey } `
        -ContentType 'application/json' -Body $alertBody
    Write-Host "   alert $($alert.alert_id): $($alert.shards_published)/$($alert.shards_targeted) shards published, $($alert.devices_in_radius) registered device(s) in the circle"

    Step "waiting for the alert on the WebSocket"
    $buffer = New-Object byte[] 16384
    $segment = [System.ArraySegment[byte]]::new($buffer)
    $text = ''
    do {
        $received = $ws.ReceiveAsync($segment, $cts.Token).GetAwaiter().GetResult()
        if ($received.MessageType -eq [System.Net.WebSockets.WebSocketMessageType]::Close) {
            Fail "the server closed the connection before an alert arrived"
        }
        $text += [Text.Encoding]::UTF8.GetString($buffer, 0, $received.Count)
    } until ($received.EndOfMessage)

    $pushed = $text | ConvertFrom-Json
    if ($pushed.id -ne $alert.alert_id) { Fail "received a different alert ($($pushed.id))" }
    $elapsed = [int]([DateTime]::UtcNow - $sent).TotalMilliseconds
    Write-Host "   received '$($pushed.title)' (severity: $($pushed.severity)) $elapsed ms after sending" -ForegroundColor Green

    Start-Sleep -Milliseconds 800
    $details = Invoke-RestMethod "$Coordinator/alerts/$($alert.alert_id)" -Headers @{ 'X-API-Key' = $operatorKey }
    if ($details.live_stats) {
        Write-Host "   workers report: delivered=$($details.live_stats.delivered) dropped=$($details.live_stats.dropped) filtered_out=$($details.live_stats.filtered_out)"
    }

    Write-Host "`nOK: register -> ticket -> WebSocket -> alert -> delivery all work." -ForegroundColor Green
    $exitCode = 0
}
catch {
    Write-Host "FAILED: $($_.Exception.Message)" -ForegroundColor Red
    $exitCode = 1
}
finally {
    if ($ws -and $ws.State -eq [System.Net.WebSockets.WebSocketState]::Open) {
        try {
            $closeCts = New-Object System.Threading.CancellationTokenSource
            $closeCts.CancelAfter([TimeSpan]::FromSeconds(3))
            $ws.CloseAsync([System.Net.WebSockets.WebSocketCloseStatus]::NormalClosure, 'done', $closeCts.Token).GetAwaiter().GetResult()
        } catch { }
    }
    if ($deviceAuth) {
        try { Invoke-RestMethod -Method Delete "$Coordinator/devices/me" -Headers $deviceAuth | Out-Null } catch { }
    }
}
exit $exitCode
