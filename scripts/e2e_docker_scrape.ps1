param(
    [string]$Query = "Restaurante Casa Pepe Madrid",
    [switch]$UseXvfb,
    [switch]$UseWorker,
    [switch]$NoBuild,
    [switch]$NoCache,
    [switch]$ForceBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Push-Location $repoRoot
try {
    if ($UseXvfb) {
        $env:SCRAPER_USE_XVFB_DOCKER = "true"
        $env:SCRAPER_HEADLESS_DOCKER = "false"
    }

    $imageTag = "review-llm-app:latest"
    $imageExists = $false
    docker image inspect $imageTag *> $null
    if ($LASTEXITCODE -eq 0) {
        $imageExists = $true
    }

    if (-not $NoBuild) {
        $shouldBuild = $ForceBuild -or $NoCache -or (-not $imageExists)
        if ($shouldBuild) {
            $buildArgs = @("compose", "build", "--progress", "plain")
            if ($NoCache) {
                $buildArgs += "--no-cache"
            }
            $buildArgs += "app"
            docker @buildArgs | Out-Host
        }
        else {
            Write-Host "Using existing image $imageTag (skip build). Use -ForceBuild to rebuild."
        }
    }

    $upArgs = @("compose")
    if ($UseWorker) {
        $upArgs += @("--profile", "worker")
    }
    $upArgs += @("up", "-d", "mongodb", "app")
    if ($UseWorker) {
        $upArgs += "scraper-worker"
    }
    docker @upArgs | Out-Host

    $healthUrl = "http://localhost:8000/health"
    $isReady = $false
    for ($i = 0; $i -lt 45; $i++) {
        try {
            $health = Invoke-RestMethod -Method Get -Uri $healthUrl -TimeoutSec 5
            if ($health -and ($health.status -eq "ok")) {
                $isReady = $true
                break
            }
        }
        catch {
        }
        Start-Sleep -Seconds 2
    }

    if (-not $isReady) {
        throw "API is not healthy after waiting for startup."
    }

    $payload = @{
        name  = $Query
        force = $true
    } | ConvertTo-Json -Depth 4

    if ($UseWorker) {
        $job = Invoke-RestMethod `
            -Method Post `
            -Uri "http://localhost:8000/business/analyze/queue" `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        if (-not $job.job_id) {
            throw "Worker enqueue failed: missing job_id."
        }

        $result = $null
        $done = $false
        for ($i = 0; $i -lt 120; $i++) {
            Start-Sleep -Seconds 2
            $state = Invoke-RestMethod `
                -Method Get `
                -Uri ("http://localhost:8000/business/analyze/queue/{0}" -f $job.job_id) `
                -TimeoutSec 20
            if ($state.status -eq "done") {
                $result = $state.result
                $done = $true
                break
            }
            if ($state.status -eq "failed") {
                throw ("Worker job failed: {0}" -f $state.error)
            }
        }
        if (-not $done) {
            throw ("Worker job timeout for job_id={0}" -f $job.job_id)
        }
    }
    else {
        $result = Invoke-RestMethod `
            -Method Post `
            -Uri "http://localhost:8000/business/analyze" `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 600
    }

    if (-not $result.business_id) {
        throw "E2E scrape failed: missing business_id in response."
    }

    Write-Host "E2E OK"
    Write-Host ("Business: {0}" -f $result.name)
    Write-Host ("Business ID: {0}" -f $result.business_id)
    Write-Host ("Strategy: {0}" -f $result.strategy)
    Write-Host ("Review count: {0}" -f $result.review_count)
    Write-Host ("Cached: {0}" -f $result.cached)

    if ($UseWorker) {
        Write-Host "Flow: worker queue"
    }
    else {
        Write-Host "Flow: direct API"
    }

    if ($result.review_count -lt 1) {
        Write-Warning "Review count is low. This can happen with limited Google Maps view in unauthenticated sessions."
    }
}
finally {
    Pop-Location
}
