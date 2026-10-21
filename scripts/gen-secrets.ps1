<#
.SYNOPSIS
  Creates or updates .env (or .env.docker) with fresh secrets.

.DESCRIPTION
  - Creates the target from its template if it does not exist.
  - If it exists, appends any keys that are new in the template.
  - Replaces JWT_SECRET and OPERATOR_API_KEYS when they are still
    placeholders (or always, with -RotateSecrets).
  - With -Ed25519, generates secrets/jwt_private.pem + jwt_public.pem
    (needs openssl; the one bundled with Git for Windows works).

.EXAMPLE
  .\scripts\gen-secrets.ps1
  .\scripts\gen-secrets.ps1 -Docker -Ed25519
#>
[CmdletBinding()]
param(
    [switch]$Docker,
    [switch]$Ed25519,
    [switch]$RotateSecrets
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$utf8 = New-Object System.Text.UTF8Encoding $false

function New-HexSecret([int]$Bytes) {
    $buf = New-Object byte[] $Bytes
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($buf) } finally { $rng.Dispose() }
    return (($buf | ForEach-Object { $_.ToString('x2') }) -join '')
}

function Get-EnvValue([string[]]$Lines, [string]$Key) {
    foreach ($l in $Lines) {
        if ($l -match "^$Key=(.*)$") { return $Matches[1].Trim() }
    }
    return $null
}

function Set-EnvValue($Lines, [string]$Key, [string]$Value) {
    for ($i = 0; $i -lt $Lines.Count; $i++) {
        if ($Lines[$i] -match "^\s*#?\s*$Key=") {
            $Lines[$i] = "$Key=$Value"
            return
        }
    }
    $Lines.Add("$Key=$Value")
}

function Test-Placeholder([string]$Value) {
    return ([string]::IsNullOrEmpty($Value) -or $Value -match '^change-me')
}

Push-Location $root
try {
    $templateName = if ($Docker) { '.env.docker.example' } else { '.env.example' }
    $targetName   = if ($Docker) { '.env.docker' } else { '.env' }
    $templatePath = Join-Path $root $templateName
    $targetPath   = Join-Path $root $targetName

    if (-not (Test-Path $templatePath)) { throw "Template $templateName not found." }

    $templateLines = [System.IO.File]::ReadAllLines($templatePath)
    $lines = New-Object 'System.Collections.Generic.List[string]'

    if (Test-Path $targetPath) {
        $lines.AddRange([string[]][System.IO.File]::ReadAllLines($targetPath))
        foreach ($t in $templateLines) {
            if ($t -match '^([A-Z][A-Z0-9_]*)=') {
                $k = $Matches[1]
                if ($null -eq (Get-EnvValue $lines.ToArray() $k)) {
                    $lines.Add($t)
                    Write-Host "added missing key: $k"
                }
            }
        }
    } else {
        $lines.AddRange([string[]]$templateLines)
        Write-Host "created $targetName from $templateName"
    }

    $current = $lines.ToArray()
    if ($RotateSecrets -or (Test-Placeholder (Get-EnvValue $current 'JWT_SECRET'))) {
        Set-EnvValue $lines 'JWT_SECRET' (New-HexSecret 48)
        Write-Host 'generated JWT_SECRET'
    }
    if ($RotateSecrets -or (Test-Placeholder (Get-EnvValue $current 'OPERATOR_API_KEYS'))) {
        Set-EnvValue $lines 'OPERATOR_API_KEYS' (New-HexSecret 32)
        Write-Host 'generated OPERATOR_API_KEYS'
    }

    if ($Ed25519) {
        $opensslPath = $null
        $found = Get-Command openssl -ErrorAction SilentlyContinue
        if ($found) {
            $opensslPath = $found.Source
        } else {
            $candidates = @(
                (Join-Path $env:ProgramFiles 'Git\usr\bin\openssl.exe'),
                (Join-Path ${env:ProgramFiles(x86)} 'Git\usr\bin\openssl.exe')
            )
            foreach ($c in $candidates) {
                if ($c -and (Test-Path $c)) { $opensslPath = $c; break }
            }
        }
        if (-not $opensslPath) {
            throw 'openssl not found. Install Git for Windows (bundles openssl) or add openssl to PATH.'
        }

        New-Item -ItemType Directory -Force -Path (Join-Path $root 'secrets') | Out-Null
        $priv = Join-Path $root 'secrets/jwt_private.pem'
        $pub  = Join-Path $root 'secrets/jwt_public.pem'
        if ($RotateSecrets -or -not (Test-Path $priv)) {
            & $opensslPath genpkey -algorithm ED25519 -out $priv
            if ($LASTEXITCODE -ne 0) { throw 'openssl genpkey failed' }
            & $opensslPath pkey -in $priv -pubout -out $pub
            if ($LASTEXITCODE -ne 0) { throw 'openssl pkey failed' }
            Write-Host 'generated Ed25519 keypair in secrets/'
        }
        if ($Docker) {
            Set-EnvValue $lines 'JWT_PRIVATE_KEY_PATH' '/run/secrets/jwt_private.pem'
            Set-EnvValue $lines 'JWT_PUBLIC_KEY_PATH' '/run/secrets/jwt_public.pem'
        } else {
            Set-EnvValue $lines 'JWT_PRIVATE_KEY_PATH' 'secrets/jwt_private.pem'
            Set-EnvValue $lines 'JWT_PUBLIC_KEY_PATH' 'secrets/jwt_public.pem'
        }
    }

    [System.IO.File]::WriteAllText($targetPath, (($lines -join "`n") + "`n"), $utf8)
    Write-Host "done: $targetName updated (secrets are in the file, not printed)."
}
finally {
    Pop-Location
}
