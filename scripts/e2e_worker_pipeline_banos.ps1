param(
    [string]$Query = "Dobuss group",
    [string]$Strategy = "scroll_copy",
    [bool]$Force = $true,
    [string]$ForceMode = "fallback_existing",
    [int]$PollSeconds = 3,
    [int]$TimeoutSeconds = 900,
    [switch]$NoBuild,
    [switch]$NoCache
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Push-Location $repoRoot
try {
    if (-not $NoBuild) {
        $buildArgs = @("compose", "--profile", "worker", "build")
        if ($NoCache) {
            $buildArgs += "--no-cache"
        }
        docker @buildArgs | Out-Host
    }

    docker compose --profile worker up -d mongodb app scraper-google-worker scraper-tripadvisor-worker analysis-worker | Out-Host

    $healthUrl = "http://localhost:8000/health"
    $isReady = $false
    for ($i = 0; $i -lt 60; $i++) {
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
        name = $Query
        force = [bool]$Force
        strategy = $Strategy
        force_mode = $ForceMode
    } | ConvertTo-Json -Depth 8

    $job = Invoke-RestMethod `
        -Method Post `
        -Uri "http://localhost:8000/business/scrape/jobs" `
        -ContentType "application/json" `
        -Body $payload `
        -TimeoutSec 30

    if (-not $job.job_id) {
        throw "Queue enqueue failed: missing job_id."
    }

    Write-Host ("Job queued: {0}" -f $job.job_id)
    Write-Host ("Query: {0}" -f $Query)
    Write-Host ("Force: {0}" -f $Force)
    Write-Host ("Strategy: {0}" -f $Strategy)
    Write-Host ("ForceMode: {0}" -f $ForceMode)

    $statusUrl = "http://localhost:8000/business/scrape/jobs/$($job.job_id)"
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastStatus = ""
    $lastStage = ""
    $finalState = $null

    while ((Get-Date) -lt $deadline) {
        $state = Invoke-RestMethod -Method Get -Uri $statusUrl -TimeoutSec 20
        $currentStatus = [string]$state.status
        $currentStage = ""
        if ($state.progress -and $state.progress.stage) {
            $currentStage = [string]$state.progress.stage
        }

        if (($currentStatus -ne $lastStatus) -or ($currentStage -ne $lastStage)) {
            Write-Host ("status={0} stage={1} updated_at={2}" -f $currentStatus, $currentStage, $state.updated_at)
            $lastStatus = $currentStatus
            $lastStage = $currentStage
        }

        if ($currentStatus -eq "done" -or $currentStatus -eq "failed") {
            $finalState = $state
            break
        }

        Start-Sleep -Seconds $PollSeconds
    }

    if (-not $finalState) {
        throw ("Timeout waiting for job completion (job_id={0})." -f $job.job_id)
    }

    $events = @()
    if ($finalState.events) {
        $events = @($finalState.events)
    }
    $stages = @($events | ForEach-Object { [string]$_.stage })

    $hasHandoff = $stages -contains "handoff_analysis_queued"
    $hasAnalysisStart = $stages -contains "analysis_worker_started"
    $hasAnalysisSummary = $stages -contains "analysis_worker_summary"

    Write-Host "=== Pipeline Checks ==="
    Write-Host ("handoff_analysis_queued: {0}" -f $hasHandoff)
    Write-Host ("analysis_worker_started: {0}" -f $hasAnalysisStart)
    Write-Host ("analysis_worker_summary: {0}" -f $hasAnalysisSummary)

    if ($finalState.status -eq "failed") {
        Write-Host ("Job failed: {0}" -f $finalState.error)
        docker compose --profile worker logs --tail 120 scraper-google-worker scraper-tripadvisor-worker analysis-worker app | Out-Host
        throw ("Pipeline failed for job_id={0}" -f $job.job_id)
    }

    $result = $finalState.result
    if (-not $result) {
        throw "Job ended as done but result payload is missing."
    }

    Write-Host "=== Result Summary ==="
    Write-Host ("business_id: {0}" -f $result.business_id)
    Write-Host ("name: {0}" -f $result.name)
    Write-Host ("review_count: {0}" -f $result.review_count)
    Write-Host ("processed_review_count: {0}" -f $result.processed_review_count)
    Write-Host ("listing_total_reviews: {0}" -f $result.listing_total_reviews)
    if ($result.analysis -and $result.analysis.meta) {
        Write-Host ("analysis_meta: {0}" -f ($result.analysis.meta | ConvertTo-Json -Compress -Depth 8))
    }

    if (-not $hasHandoff -or -not $hasAnalysisStart) {
        throw "Pipeline completed but required handoff stages were not found in events."
    }

    Write-Host "E2E worker pipeline OK."
}
finally {
    Pop-Location
}
