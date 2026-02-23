param(
    [ValidateSet("up", "down", "status", "logs")]
    [string]$Action = "up"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Compose {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args
    )
    & docker compose @Args
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose $($Args -join ' ') failed with exit code $LASTEXITCODE."
    }
}

function Get-MongoContainerId {
    $id = (& docker compose ps -q mongodb).Trim()
    return $id
}

function Wait-MongoHealthy {
    param(
        [int]$TimeoutSeconds = 60
    )
    $id = Get-MongoContainerId
    if (-not $id) {
        throw "Could not resolve mongodb container id."
    }

    $start = Get-Date
    while (((Get-Date) - $start).TotalSeconds -lt $TimeoutSeconds) {
        $state = (& docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $id).Trim()
        if ($state -eq "healthy" -or $state -eq "running") {
            return
        }
        Start-Sleep -Seconds 1
    }

    throw "MongoDB did not become healthy within $TimeoutSeconds seconds."
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Push-Location $repoRoot
try {
    switch ($Action) {
        "up" {
            Invoke-Compose -Args @("up", "-d", "mongodb")
            Wait-MongoHealthy -TimeoutSeconds 60
            Write-Host "MongoDB is up and healthy."
            Write-Host "Now run API locally:"
            Write-Host "uv run uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload"
        }
        "down" {
            Invoke-Compose -Args @("stop", "mongodb")
            Write-Host "MongoDB stopped."
        }
        "status" {
            Invoke-Compose -Args @("ps", "mongodb")
        }
        "logs" {
            Invoke-Compose -Args @("logs", "-f", "mongodb")
        }
    }
}
finally {
    Pop-Location
}
