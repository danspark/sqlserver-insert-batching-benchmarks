[CmdletBinding()]
param(
  [string] $OutputPath = (Join-Path $PSScriptRoot "../config/Full.json")
)

$ErrorActionPreference = "Stop"
$seed = 1592606758
$scenarios = [System.Collections.Generic.List[object]]::new()

function Add-Scenario {
  param(
    [string] $Name,
    [string] $Stage,
    [string] $Mode = "Queue",
    [string] $Strategy = "BulkCopy",
    [string] $Batching = "Channel",
    [string] $SqlExecution = "Native",
    [string] $Distribution = "Uniform",
    [int] $Rows = 10000,
    [int] $Workers = 1,
    [int] $Writers = 2,
    [int] $BatchSize = 500,
    [int] $DelayMs = 5,
    [int] $Capacity = 4000,
    [int] $Prefetch = 1000,
    [int] $SqlBatchCommands = 1,
    [int] $SqlBatchDelayMs = 1,
    [int] $SqlBatchRequests = 1,
    [int] $Repetition = 1
  )

  $scenarios.Add([ordered]@{
    name = $Name
    stage = $Stage
    mode = $Mode
    strategy = $Strategy
    batching = $Batching
    sqlExecution = $SqlExecution
    distribution = $Distribution
    seed = $seed
    rowCount = $Rows
    workerInstances = $Workers
    writersPerInstance = $Writers
    batchSize = $BatchSize
    maximumBatchingDelayMilliseconds = $DelayMs
    channelCapacity = $Capacity
    rabbitMqPrefetch = $Prefetch
    sqlBatchMaximumCommands = $SqlBatchCommands
    sqlBatchMaximumDelayMilliseconds = $SqlBatchDelayMs
    sqlBatchRequestConcurrency = $SqlBatchRequests
    repetition = $Repetition
  })
}

Add-Scenario -Name "control-noop-queue" -Stage "control" -Mode "NoOpQueue" -Rows 100000 -Workers 4 -Writers 4 -BatchSize 1000 -DelayMs 1 -Capacity 16000 -Prefetch 4000

$strategySettings = [ordered]@{
  Individual = @{ Batch = 1; Batching = "None"; Writers = 4; Capacity = 1000; Prefetch = 250 }
  TableValuedParameter = @{ Batch = 500; Batching = "Channel"; Writers = 2; Capacity = 4000; Prefetch = 1000 }
  MultipleInsertStatements = @{ Batch = 50; Batching = "Channel"; Writers = 2; Capacity = 2000; Prefetch = 200 }
  MultiRowValues = @{ Batch = 100; Batching = "Channel"; Writers = 2; Capacity = 2000; Prefetch = 400 }
  BulkCopy = @{ Batch = 1000; Batching = "Channel"; Writers = 2; Capacity = 8000; Prefetch = 2000 }
}

foreach ($distribution in @("Uniform", "ModerateSkew", "HotParent")) {
  foreach ($entry in $strategySettings.GetEnumerator()) {
    $setting = $entry.Value
    Add-Scenario -Name "broad-$($entry.Key.ToLowerInvariant())-$($distribution.ToLowerInvariant())" -Stage "broad" `
      -Strategy $entry.Key -Batching $setting.Batching -Distribution $distribution -Writers $setting.Writers `
      -BatchSize $setting.Batch -Capacity $setting.Capacity -Prefetch $setting.Prefetch
  }
}

foreach ($strategy in @("TableValuedParameter", "BulkCopy")) {
  foreach ($batchSize in @(10, 50, 100, 500, 1000, 5000)) {
    $capacity = [Math]::Max(8000, $batchSize * 2)
    Add-Scenario -Name "refine-$($strategy.ToLowerInvariant())-batch-$batchSize" -Stage "refine-batch" `
      -Strategy $strategy -BatchSize $batchSize -Capacity $capacity -Prefetch ([Math]::Min(65535, $batchSize * 4))
  }
}

foreach ($strategy in @("MultipleInsertStatements", "MultiRowValues")) {
  foreach ($batchSize in @(10, 50, 100, 161)) {
    Add-Scenario -Name "refine-$($strategy.ToLowerInvariant())-batch-$batchSize" -Stage "refine-batch" `
      -Strategy $strategy -BatchSize $batchSize -Capacity 2000 -Prefetch ([Math]::Min(65535, $batchSize * 4))
  }
}

foreach ($delay in @(1, 5, 20, 50)) {
  Add-Scenario -Name "refine-bulk-delay-$delay" -Stage "refine-delay" -DelayMs $delay
}

foreach ($capacity in @(500, 2000, 8000)) {
  Add-Scenario -Name "refine-bulk-capacity-$capacity" -Stage "refine-capacity" -Capacity $capacity
}

foreach ($prefetch in @(500, 1000, 2000, 4000)) {
  Add-Scenario -Name "refine-bulk-prefetch-$prefetch" -Stage "refine-prefetch" -Prefetch $prefetch
}

foreach ($batching in @("Channel", "LockSwap")) {
  foreach ($strategy in @("TableValuedParameter", "BulkCopy")) {
    Add-Scenario -Name "refine-$($strategy.ToLowerInvariant())-$($batching.ToLowerInvariant())" -Stage "refine-batcher" `
      -Strategy $strategy -Batching $batching
  }
}

foreach ($writers in @(1, 2, 4, 8)) {
  Add-Scenario -Name "refine-bulk-writers-$writers" -Stage "refine-concurrency" -Writers $writers
}

$sqlBatchSettings = [ordered]@{
  Individual = @{ Batch = 1; Batching = "None"; Capacity = 1000; Prefetch = 250 }
  TableValuedParameter = @{ Batch = 500; Batching = "Channel"; Capacity = 4000; Prefetch = 2000 }
  MultipleInsertStatements = @{ Batch = 50; Batching = "Channel"; Capacity = 2000; Prefetch = 400 }
  MultiRowValues = @{ Batch = 100; Batching = "Channel"; Capacity = 2000; Prefetch = 800 }
}

foreach ($entry in $sqlBatchSettings.GetEnumerator()) {
  $setting = $entry.Value
  Add-Scenario -Name "sqlbatch-$($entry.Key.ToLowerInvariant())-single-command" -Stage "sqlbatch-control" `
    -Strategy $entry.Key -Batching $setting.Batching -SqlExecution "SqlBatch" -Writers 1 `
    -BatchSize $setting.Batch -Capacity $setting.Capacity -Prefetch $setting.Prefetch `
    -SqlBatchCommands 1

  foreach ($writers in @(2, 4, 8)) {
    Add-Scenario -Name "sqlbatch-$($entry.Key.ToLowerInvariant())-writers-$writers" -Stage "sqlbatch-writers" `
      -Strategy $entry.Key -Batching $setting.Batching -SqlExecution "SqlBatch" -Writers $writers `
      -BatchSize $setting.Batch -Capacity $setting.Capacity -Prefetch $setting.Prefetch `
      -SqlBatchCommands $writers
  }

  foreach ($commands in @(1, 2, 4, 8)) {
    Add-Scenario -Name "sqlbatch-$($entry.Key.ToLowerInvariant())-commands-$commands" -Stage "sqlbatch-commands" `
      -Strategy $entry.Key -Batching $setting.Batching -SqlExecution "SqlBatch" -Writers 8 `
      -BatchSize $setting.Batch -Capacity $setting.Capacity -Prefetch $setting.Prefetch `
      -SqlBatchCommands $commands
  }

  Add-Scenario -Name "direct-sqlbatch-$($entry.Key.ToLowerInvariant())" -Stage "direct-control" `
    -Mode "DirectDatabase" -Strategy $entry.Key -Batching $setting.Batching -SqlExecution "SqlBatch" `
    -Writers 8 -BatchSize $setting.Batch -Capacity $setting.Capacity -Prefetch $setting.Prefetch `
    -SqlBatchCommands 8

}

foreach ($delay in @(1, 5, 20)) {
  Add-Scenario -Name "sqlbatch-individual-delay-$delay" -Stage "sqlbatch-delay" `
    -Strategy "Individual" -Batching "None" -SqlExecution "SqlBatch" -Writers 4 `
    -BatchSize 1 -Capacity 1000 -Prefetch 250 -SqlBatchCommands 4 -SqlBatchDelayMs $delay
}

foreach ($entry in $strategySettings.GetEnumerator()) {
  $setting = $entry.Value
  foreach ($workers in @(1, 2, 4)) {
    Add-Scenario -Name "scaling-$($entry.Key.ToLowerInvariant())-workers-$workers" -Stage "scaling" `
      -Strategy $entry.Key -Batching $setting.Batching -Workers $workers -Writers $setting.Writers `
      -BatchSize $setting.Batch -Capacity $setting.Capacity -Prefetch $setting.Prefetch
  }
}

foreach ($entry in $strategySettings.GetEnumerator()) {
  $setting = $entry.Value
  Add-Scenario -Name "direct-$($entry.Key.ToLowerInvariant())" -Stage "direct-control" -Mode "DirectDatabase" `
    -Strategy $entry.Key -Batching $setting.Batching -Writers $setting.Writers -BatchSize $setting.Batch `
    -Capacity $setting.Capacity -Prefetch $setting.Prefetch
}

foreach ($entry in $strategySettings.GetEnumerator()) {
  $setting = $entry.Value
  foreach ($repetition in 1..3) {
    Add-Scenario -Name "finalist-$($entry.Key.ToLowerInvariant())" -Stage "finalist" -Rows 250000 `
      -Strategy $entry.Key -Batching $setting.Batching -Workers 1 -Writers $setting.Writers `
      -BatchSize $setting.Batch -Capacity $setting.Capacity -Prefetch $setting.Prefetch -Repetition $repetition
  }
}

$profile = [ordered]@{
  name = "Full"
  warmupRows = 1000
  scenarios = $scenarios
}

$outputDirectory = Split-Path -Parent $OutputPath
New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
$profile | ConvertTo-Json -Depth 8 | Set-Content -Path $OutputPath -Encoding utf8NoBOM
Write-Host "Wrote $($scenarios.Count) scenarios to $OutputPath"
