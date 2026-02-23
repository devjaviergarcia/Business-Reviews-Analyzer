param(
    [switch]$Reload
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Push-Location $repoRoot
try {
    if ($Reload) {
        Write-Warning "On Windows, --reload can break Playwright scraper endpoints. Use without -Reload for /business/analyze."
        uv run uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
    }
    else {
        uv run uvicorn src.main:app --host 0.0.0.0 --port 8000
    }
}
finally {
    Pop-Location
}
