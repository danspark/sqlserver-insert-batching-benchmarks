[CmdletBinding()]
param(
  [ValidateSet("Smoke", "Full")]
  [string] $Profile = "Smoke",
  [string] $Scenario,
  [ValidateRange(1, 100000000)]
  [int] $Rows,
  [string] $OutputDirectory,
  [switch] $LeaveRunning,
  [switch] $UseRunningEnvironment
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot "compose.yaml"
$profilePath = Join-Path $repoRoot "config/$Profile.json"
$createdAllResources = $false
$newContainerIds = @()
$statisticsJob = $null
$statisticsStopFile = $null
$previousLocation = Get-Location

function Assert-Command([string] $Name) {
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Required command '$Name' was not found on PATH."
  }
}

function Wait-Healthy([string] $Service, [TimeSpan] $Timeout) {
  $timer = [Diagnostics.Stopwatch]::StartNew()
  while ($timer.Elapsed -lt $Timeout) {
    $containerId = (& docker compose -f $composeFile ps -q $Service).Trim()
    if ($containerId) {
      $health = (& docker inspect --format '{{.State.Health.Status}}' $containerId).Trim()
      if ($health -eq "healthy") {
        Write-Host "$Service is healthy."
        return
      }
      if ($health -eq "unhealthy") {
        & docker compose -f $composeFile logs $Service
        throw "$Service reported an unhealthy container state."
      }
    }
    Start-Sleep -Milliseconds 500
  }
  throw "Timed out waiting for $Service health after $($Timeout.TotalSeconds) seconds."
}

function New-BenchmarkSecret([string] $Prefix) {
  return "$Prefix$([Guid]::NewGuid().ToString('N'))Aa1"
}

try {
  Set-Location $repoRoot
  Assert-Command "dotnet"
  Assert-Command "docker"
  Assert-Command "pwsh"
  & docker compose version | Out-Null
  & docker info --format '{{.ServerVersion}}' | Out-Null

  if ($Profile -eq "Full") {
    & pwsh -NoLogo -NoProfile (Join-Path $PSScriptRoot "generate-full-profile.ps1")
  }

  Write-Host "Building the complete solution..."
  & dotnet build (Join-Path $repoRoot "SqlServerInsertBatchingBenchmarks.slnx") -c Release
  if ($LASTEXITCODE -ne 0) { throw "Solution build failed." }

  if (-not $UseRunningEnvironment) {
    $env:SQLBENCH_SA_PASSWORD = New-BenchmarkSecret "SqlBenchSql_"
    $env:SQLBENCH_RABBIT_USER = "sqlbench"
    $env:SQLBENCH_RABBIT_PASSWORD = New-BenchmarkSecret "SqlBenchRabbit_"
    $escapedUser = [Uri]::EscapeDataString($env:SQLBENCH_RABBIT_USER)
    $escapedPassword = [Uri]::EscapeDataString($env:SQLBENCH_RABBIT_PASSWORD)
    $env:SQLBENCH_SQL_CONNECTION = "Server=localhost,14333;User ID=sa;Password=$($env:SQLBENCH_SA_PASSWORD);Encrypt=True;TrustServerCertificate=True;Pooling=True;Min Pool Size=0;Max Pool Size=200;Connect Timeout=30"
    $env:SQLBENCH_RABBIT_CONNECTION = "amqp://${escapedUser}:${escapedPassword}@localhost:5673/%2f"
    $env:SQLBENCH_RABBIT_MANAGEMENT = "http://localhost:15673"
    $env:SQLBENCH_QUEUE = "sqlbench.messages"
  } else {
    foreach ($name in @("SQLBENCH_SQL_CONNECTION", "SQLBENCH_RABBIT_CONNECTION", "SQLBENCH_RABBIT_MANAGEMENT", "SQLBENCH_RABBIT_USER", "SQLBENCH_RABBIT_PASSWORD")) {
      if (-not [Environment]::GetEnvironmentVariable($name)) {
        throw "-UseRunningEnvironment requires $name in the current process environment."
      }
    }
  }

  $beforeIds = @(& docker compose -f $composeFile ps -aq) | Where-Object { $_ }
  if (-not $UseRunningEnvironment) {
    Write-Host "Starting pinned SQL Server and RabbitMQ containers..."
    & docker compose -f $composeFile up -d sqlserver rabbitmq
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose startup failed." }
    $afterIds = @(& docker compose -f $composeFile ps -aq) | Where-Object { $_ }
    $newContainerIds = @($afterIds | Where-Object { $beforeIds -notcontains $_ })
    $createdAllResources = $beforeIds.Count -eq 0
  }

  Wait-Healthy "sqlserver" ([TimeSpan]::FromMinutes(5))
  Wait-Healthy "rabbitmq" ([TimeSpan]::FromMinutes(3))

  Write-Host "Running unit and container-backed integration tests..."
  & dotnet test (Join-Path $repoRoot "SqlServerInsertBatchingBenchmarks.slnx") -c Release --no-build
  if ($LASTEXITCODE -ne 0) { throw "Tests failed." }

  if (-not $OutputDirectory) {
    $stamp = [DateTimeOffset]::UtcNow.ToString("yyyyMMdd-HHmmss")
    $OutputDirectory = Join-Path $repoRoot "results/local/$stamp-$($Profile.ToLowerInvariant())"
  }
  $OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
  New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null

  $environment = [ordered]@{
    recordedAt = [DateTimeOffset]::UtcNow.ToString("O")
    operatingSystem = [Runtime.InteropServices.RuntimeInformation]::OSDescription
    architecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
    processorCount = [Environment]::ProcessorCount
    dotnetSdk = (& dotnet --version).Trim()
    powerShell = $PSVersionTable.PSVersion.ToString()
    docker = (& docker version --format '{{.Server.Version}}').Trim()
    dockerCompose = (& docker compose version --short).Trim()
    aspire = (& aspire --version).Trim()
    sqlServerLimit = @{ cpus = 4; memoryBytes = 4GB }
    rabbitMqLimit = @{ cpus = 2; memoryBytes = 1GB }
    profile = $Profile
    scenarioFilter = $Scenario
    rowOverride = if ($PSBoundParameters.ContainsKey("Rows")) { $Rows } else { $null }
  }
  $environment | ConvertTo-Json -Depth 6 | Set-Content (Join-Path $OutputDirectory "environment.json") -Encoding utf8NoBOM

  $statisticsStopFile = Join-Path ([IO.Path]::GetTempPath()) "sqlbench-stats-$([Guid]::NewGuid().ToString('N')).stop"
  $statisticsPath = Join-Path $OutputDirectory "container-stats.jsonl"
  $statisticsJob = Start-Job -ScriptBlock {
    param($ComposePath, $StopFile, $StatsPath)
    while (-not (Test-Path $StopFile)) {
      $timestamp = [DateTimeOffset]::UtcNow.ToString("O")
      $containerIds = @(& docker compose -f $ComposePath ps -q sqlserver rabbitmq) | Where-Object { $_ }
      if ($containerIds.Count -gt 0) {
        $samples = & docker stats --no-stream --format '{{json .}}' $containerIds
        foreach ($sample in $samples) {
          @{ timestamp = $timestamp; sample = ($sample | ConvertFrom-Json) } |
            ConvertTo-Json -Compress -Depth 5 |
            Add-Content -Path $StatsPath -Encoding utf8
        }
      }
      Start-Sleep -Seconds 1
    }
  } -ArgumentList $composeFile, $statisticsStopFile, $statisticsPath

  $controller = Join-Path $repoRoot "src/SqlBench.Controller/bin/Release/net10.0/SqlBench.Controller.dll"
  $worker = Join-Path $repoRoot "src/SqlBench.Worker/bin/Release/net10.0/SqlBench.Worker.dll"
  $controllerArguments = @($controller, "--profile", $profilePath, "--output", $OutputDirectory, "--worker", $worker)
  if ($Scenario) { $controllerArguments += @("--scenario", $Scenario) }
  if ($PSBoundParameters.ContainsKey("Rows")) { $controllerArguments += @("--rows", $Rows.ToString()) }
  & dotnet @controllerArguments
  $controllerExit = $LASTEXITCODE

  if ($statisticsJob) {
    New-Item -ItemType File -Path $statisticsStopFile -Force | Out-Null
    Wait-Job $statisticsJob -Timeout 10 | Out-Null
    Remove-Job $statisticsJob -Force
    $statisticsJob = $null
  }

  $workPath = Join-Path $OutputDirectory ".work"
  if (Test-Path $workPath) {
    Remove-Item -LiteralPath $workPath -Recurse -Force
  }

  $resultFiles = @(Get-ChildItem -Path $OutputDirectory -Filter "*.json" -File |
    Where-Object { $_.Name -notin @("environment.json", "manifest.json") })
  if ($resultFiles.Count -gt 0) {
    & dotnet run --project (Join-Path $repoRoot "src/SqlBench.Report/SqlBench.Report.csproj") -c Release --no-build -- `
      --results $OutputDirectory --readme (Join-Path $repoRoot "README.md") --charts (Join-Path $repoRoot "docs/charts")
    $reportExit = $LASTEXITCODE
  } else {
    $reportExit = 1
  }

  if ($controllerExit -ne 0 -or $reportExit -ne 0) {
    throw "Benchmark or report generation failed. Raw diagnostics remain in $OutputDirectory."
  }

  Write-Host "Benchmark results: $OutputDirectory"
} finally {
  if ($statisticsJob) {
    if ($statisticsStopFile) { New-Item -ItemType File -Path $statisticsStopFile -Force | Out-Null }
    Stop-Job $statisticsJob -ErrorAction SilentlyContinue
    Remove-Job $statisticsJob -Force -ErrorAction SilentlyContinue
  }
  if ($statisticsStopFile -and (Test-Path $statisticsStopFile)) {
    Remove-Item -LiteralPath $statisticsStopFile -Force
  }

  if (-not $LeaveRunning -and -not $UseRunningEnvironment) {
    if ($createdAllResources) {
      & docker compose -f $composeFile down --volumes
    } else {
      foreach ($containerId in $newContainerIds) {
        if ($containerId -match '^[a-f0-9]{12,64}$') {
          & docker rm --force $containerId | Out-Null
        }
      }
    }
  }
  Set-Location $previousLocation
}
