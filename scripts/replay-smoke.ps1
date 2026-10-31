<#
.SYNOPSIS
  Shows durable delivery: a device acknowledges an alert, goes offline, a
  second alert fires while it is away, and on reconnecting the device is sent
  exactly the alert it missed (not the one it already acknowledged).

.DESCRIPTION
  Needs the coordinator and a fanout-worker that owns the device's shard, and
  OPERATOR_API_KEYS in .env (scripts\gen-secrets.ps1). Exits 0 on success.

  A device that connects with no acknowledgement history is sent the alerts of
  the last five minutes in its area, so the script first waits for that replay
  to finish (the server announces it with a replay_complete message) before it
  starts the experiment.

.EXAMPLE
  .\scripts\replay-smoke.ps1
#>
[CmdletBinding()]
param(
    [string]$Coordinator = "http://localhost:8090",
    [double]$Lat = 24.8607,
    [double]$Lon = 67.0011,
    [double]$RadiusMeters = 5000,
    [int]$TimeoutSeconds = 40
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot

function Step($text) { Write-Host "-> $text" -ForegroundColor Cyan }
function Fail($text) { Write-Host "FAILED: $text" -ForegroundColor Red; exit 1 }

$envFile = Join-Path $root '.env'
if (-not (Test-Path $envFile)) { Fail ".env not found. Run .\scripts\gen-secrets.ps1 first." }
$match = Select-String -Path $envFile -Pattern '^OPERATOR_API_KEYS=(.+)$' | Select-Object -First 1
if (-not $match) { Fail "OPERATOR_API_KEYS is not set in .env." }
$operatorKey = ($match.Matches[0].Groups[1].Value -split ',')[0].Trim()
$operator = @{ 'X-API-Key' = $operatorKey }

$cts = New-Object System.Threading.CancellationTokenSource
$cts.CancelAfter([TimeSpan]::FromSeconds($TimeoutSeconds))

function Open-Socket($deviceId, $auth) {
    $ticket = Invoke-RestMethod -Method Post "$Coordinator/devices/ws-ticket" -Headers $auth
    if (-not $ticket.ws_url) { Fail "no fanout-worker owns shard $($ticket.shard_prefix)" }
    $uri = [Uri]"$($ticket.ws_url)/connect/${deviceId}?ticket=$($ticket.ticket)"
    $socket = New-Object System.Net.WebSockets.ClientWebSocket
    $socket.ConnectAsync($uri, $cts.Token).GetAwaiter().GetResult() | Out-Null
    return $socket
}

# Reads one complete message. Returns @{ Type = 'binary' | 'text' | 'close'; Text = ... }
function Receive-Frame($socket) {
    $buffer = New-Object byte[] 16384
    $segment = [System.ArraySegment[byte]]::new($buffer)
    $stream = New-Object System.IO.MemoryStream
    do {
        $r = $socket.ReceiveAsync($segment, $cts.Token).GetAwaiter().GetResult()
        if ($r.MessageType -eq [System.Net.WebSockets.WebSocketMessageType]::Close) {
            return @{ Type = 'close'; Text = '' }
        }
        $stream.Write($buffer, 0, $r.Count)
    } until ($r.EndOfMessage)
    $kind = 'text'
    if ($r.MessageType -eq [System.Net.WebSockets.WebSocketMessageType]::Binary) { $kind = 'binary' }
    return @{ Type = $kind; Text = [Text.Encoding]::UTF8.GetString($stream.ToArray()) }
}

# Reads frames until the server announces that the connect-time replay is done.
# Returns the ids of the alerts that were replayed and the count the server reported.
function Read-Replay($socket) {
    $ids = @()
    while ($true) {
        $frame = Receive-Frame $socket
        if ($frame.Type -eq 'binary') {
            $ids += [regex]::Match($frame.Text, '"id":"([^"]+)"').Groups[1].Value
        } elseif ($frame.Type -eq 'text') {
            $control = $frame.Text | ConvertFrom-Json
            if ($control.type -eq 'replay_complete') {
                return @{ Ids = $ids; Count = $control.replayed }
            }
        } else {
            Fail "the server closed the connection during the replay"
        }
    }
}

function Send-Alert($title) {
    $body = @{
        lat = $Lat; lon = $Lon; radius_meters = $RadiusMeters
        severity = 'warning'; title = $title; message = 'replay demo'
    } | ConvertTo-Json
    return Invoke-RestMethod -Method Post "$Coordinator/alerts" -Headers $operator -ContentType 'application/json' -Body $body
}

$deviceAuth = $null
$first = $null
$second = $null
$exitCode = 1
try {
    Step "registering a device and connecting"
    $device = Invoke-RestMethod -Method Post "$Coordinator/devices/register" -ContentType 'application/json' `
        -Body (@{ lat = $Lat; lon = $Lon } | ConvertTo-Json)
    $deviceAuth = @{ Authorization = "Bearer $($device.token)" }
    $first = Open-Socket $device.device_id $deviceAuth

    Step "waiting for the connect-time replay (a new device is sent the last 5 minutes of alerts in its area)"
    $early = Read-Replay $first
    Write-Host "   the replay delivered $($early.Ids.Count) earlier alert(s); the server reported $($early.Count)"
    if ($early.Ids.Count -ne $early.Count) { Fail "the server reported $($early.Count) replayed alerts but sent $($early.Ids.Count)" }

    Step "alert A is sent while the device is online"
    $alertA = Send-Alert 'replay demo A'
    $frame = Receive-Frame $first
    if ($frame.Type -ne 'binary') { Fail "expected alert A, got a $($frame.Type) frame" }
    $idA = [regex]::Match($frame.Text, '"id":"([^"]+)"').Groups[1].Value
    $createdA = [regex]::Match($frame.Text, '"created_at":"([^"]+)"').Groups[1].Value
    if ($idA -ne $alertA.alert_id) { Fail "received alert $idA instead of A ($($alertA.alert_id))" }
    Write-Host "   received A ($idA)"

    Step "acknowledging A, then going offline"
    $ack = '{"type":"ack","alert_id":"' + $idA + '","created_at":"' + $createdA + '"}'
    $ackBytes = [System.ArraySegment[byte]]::new([Text.Encoding]::UTF8.GetBytes($ack))
    $first.SendAsync($ackBytes, [System.Net.WebSockets.WebSocketMessageType]::Text, $true, $cts.Token).GetAwaiter().GetResult() | Out-Null
    Start-Sleep -Milliseconds 1500
    # Going offline: a failed close handshake must not fail the experiment,
    # the device is simply gone either way.
    try {
        $offlineCts = New-Object System.Threading.CancellationTokenSource
        $offlineCts.CancelAfter([TimeSpan]::FromSeconds(3))
        $first.CloseAsync([System.Net.WebSockets.WebSocketCloseStatus]::NormalClosure, 'offline', $offlineCts.Token).GetAwaiter().GetResult() | Out-Null
    } catch {
        Write-Host "   (the close handshake did not complete cleanly; treating the device as offline anyway)" -ForegroundColor DarkYellow
        try { $first.Abort() } catch { }
    }
    Start-Sleep -Milliseconds 500

    Step "alert B is sent while the device is OFFLINE"
    $alertB = Send-Alert 'replay demo B'
    Write-Host "   alert B is $($alertB.alert_id)"

    Step "reconnecting and waiting for the replay"
    $second = Open-Socket $device.device_id $deviceAuth
    $result = Read-Replay $second
    $replayed = @($result.Ids)

    if ($replayed -contains $idA) { Fail "alert A was replayed even though it was acknowledged" }
    if (($replayed.Count -ne 1) -or ($replayed[0] -ne $alertB.alert_id)) {
        Fail "expected exactly alert B to be replayed, got: $($replayed -join ', ')"
    }
    if ($result.Count -ne 1) { Fail "replay_complete reported $($result.Count) alerts, expected 1" }

    Write-Host "`nOK: the device missed B while offline and was sent exactly B on reconnect;" -ForegroundColor Green
    Write-Host "    A (acknowledged) was not repeated." -ForegroundColor Green
    $exitCode = 0
}
catch {
    Write-Host "FAILED: $($_.Exception.Message)" -ForegroundColor Red
    $exitCode = 1
}
finally {
    foreach ($s in @($first, $second)) {
        if ($s -and $s.State -eq [System.Net.WebSockets.WebSocketState]::Open) {
            try {
                $closeCts = New-Object System.Threading.CancellationTokenSource
                $closeCts.CancelAfter([TimeSpan]::FromSeconds(3))
                $s.CloseAsync([System.Net.WebSockets.WebSocketCloseStatus]::NormalClosure, 'done', $closeCts.Token).GetAwaiter().GetResult() | Out-Null
            } catch { }
        }
    }
    if ($deviceAuth) {
        try { Invoke-RestMethod -Method Delete "$Coordinator/devices/me" -Headers $deviceAuth | Out-Null } catch { }
    }
}
exit $exitCode