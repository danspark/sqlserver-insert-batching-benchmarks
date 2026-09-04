[CmdletBinding()]
param(
  [string] $OutputPath = (Join-Path $PSScriptRoot "../config/SqlBatch.json")
)

$ErrorActionPreference = "Stop"
$seed = 1592606758
$scenarios = [System.Collections.Generic.List[object]]::new()

function Add-Scenario {
  param(
    [string] $Name,
    [string] $Stage,
    [string] $Mode = "Queue",
    [string] $Strategy = "Individual",
    [string] $Batching = "None",
    [string] $SqlExecution = "Native",
    [string] $Distribution = "Uniform",
    [int] $Rows = 20000,
    [int] $Workers = 1,
    [int] $Writers = 1,
    [int] $BatchSize = 1,
    [int] $DelayMs = 5,
    [int] $Capacity = 1000,
    [int] $Prefetch = 250,
    [int] $SqlBatchCommands = 1,
    [int] $SqlBatchDelayMs = 1,
    [int] $SqlBatchRequests = 1
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
    repetition = 1
  })
}

foreach ($control in @(
    @{ Name = "individual"; Batching = "None"; Writers = 4; Batch = 1; Capacity = 1000; Prefetch = 250 },
    @{ Name = "tvp"; Batching = "Channel"; Writers = 4; Batch = 500; Capacity = 4000; Prefetch = 2000 },
    @{ Name = "multiple-statements-w4"; Batching = "Channel"; Writers = 4; Batch = 50; Capacity = 2000; Prefetch = 400 },
    @{ Name = "multiple-statements"; Batching = "Channel"; Writers = 8; Batch = 50; Capacity = 2000; Prefetch = 400 },
    @{ Name = "multi-row-values-w4"; Batching = "Channel"; Writers = 4; Batch = 100; Capacity = 2000; Prefetch = 800 },
    @{ Name = "multi-row-values"; Batching = "Channel"; Writers = 8; Batch = 100; Capacity = 2000; Prefetch = 800 })) {
  Add-Scenario -Name "sqlbatch-control-noop-$($control.Name)" -Stage "control" -Mode "NoOpQueue" `
    -Strategy "BulkCopy" -Batching $control.Batching -Rows 200000 -Writers $control.Writers `
    -BatchSize $control.Batch -DelayMs 5 -Capacity $control.Capacity -Prefetch $control.Prefetch
}

$settings = [ordered]@{
  Individual = @{ Rows = 2000; Batch = 1; Batching = "None"; Capacity = 1000; Prefetch = 250 }
  TableValuedParameter = @{ Rows = 20000; Batch = 500; Batching = "Channel"; Capacity = 4000; Prefetch = 2000 }
  MultipleInsertStatements = @{ Rows = 20000; Batch = 50; Batching = "Channel"; Capacity = 2000; Prefetch = 400 }
  MultiRowValues = @{ Rows = 20000; Batch = 100; Batching = "Channel"; Capacity = 2000; Prefetch = 800 }
}

# A one-writer SqlBatch sends one command per request. It isolates SqlBatch's own
# overhead before command coalescing is enabled.
foreach ($entry in $settings.GetEnumerator()) {
  $setting = $entry.Value
  foreach ($execution in @("Native", "SqlBatch")) {
    Add-Scenario -Name "sqlbatch-control-$($entry.Key.ToLowerInvariant())-$($execution.ToLowerInvariant())" `
      -Stage "single-command-control" -Strategy $entry.Key -Batching $setting.Batching `
      -SqlExecution $execution -Rows $setting.Rows -Writers 1 -BatchSize $setting.Batch `
      -Capacity $setting.Capacity -Prefetch $setting.Prefetch -SqlBatchCommands 1
  }
}

# Concurrent request lanes retain some or all native connection concurrency while
# still allowing each request to carry more than one independently committed
# writer command.
foreach ($entry in $settings.GetEnumerator()) {
  $setting = $entry.Value
  foreach ($layout in @(
      @{ Writers = 4; Commands = 1; Requests = 4 },
      @{ Writers = 4; Commands = 2; Requests = 2 },
      @{ Writers = 8; Commands = 1; Requests = 8 },
      @{ Writers = 8; Commands = 2; Requests = 4 },
      @{ Writers = 8; Commands = 4; Requests = 2 })) {
    Add-Scenario -Name "sqlbatch-lanes-$($entry.Key.ToLowerInvariant())-w$($layout.Writers)-c$($layout.Commands)-r$($layout.Requests)" `
      -Stage "request-concurrency" -Strategy $entry.Key -Batching $setting.Batching -SqlExecution "SqlBatch" `
      -Rows $setting.Rows -Writers $layout.Writers -BatchSize $setting.Batch -Capacity $setting.Capacity `
      -Prefetch $setting.Prefetch -SqlBatchCommands $layout.Commands -SqlBatchRequests $layout.Requests
  }
}

# Writer scaling keeps all persistence settings fixed. SqlBatch may combine at
# most one command from each concurrently active writer.
foreach ($entry in $settings.GetEnumerator()) {
  $setting = $entry.Value
  foreach ($writers in @(2, 4, 8)) {
    foreach ($execution in @("Native", "SqlBatch")) {
      $maximumCommands = if ($execution -eq "SqlBatch") { $writers } else { 1 }
      Add-Scenario -Name "sqlbatch-writers-$($entry.Key.ToLowerInvariant())-$($execution.ToLowerInvariant())-$writers" `
        -Stage "writer-scaling" -Strategy $entry.Key -Batching $setting.Batching `
        -SqlExecution $execution -Rows $setting.Rows -Writers $writers -BatchSize $setting.Batch `
        -Capacity $setting.Capacity -Prefetch $setting.Prefetch -SqlBatchCommands $maximumCommands
    }
  }
}

# Command-cap refinement uses eight writers so 1, 2, 4, and 8 commands/execute
# are all reachable. The eight-command case is already present above.
foreach ($entry in $settings.GetEnumerator()) {
  $setting = $entry.Value
  foreach ($maximumCommands in @(1, 2, 4)) {
    Add-Scenario -Name "sqlbatch-command-cap-$($entry.Key.ToLowerInvariant())-$maximumCommands" `
      -Stage "command-cap" -Strategy $entry.Key -Batching $setting.Batching -SqlExecution "SqlBatch" `
      -Rows $setting.Rows -Writers 8 -BatchSize $setting.Batch -Capacity $setting.Capacity `
      -Prefetch $setting.Prefetch -SqlBatchCommands $maximumCommands
  }
}

# Delay refinement is focused on the two command-heavy strategies where saving
# round trips is most likely to compensate for an extra coalescing delay.
foreach ($strategy in @("Individual", "MultipleInsertStatements")) {
  $setting = $settings[$strategy]
  foreach ($sqlBatchDelay in @(5, 20)) {
    Add-Scenario -Name "sqlbatch-delay-$($strategy.ToLowerInvariant())-$sqlBatchDelay" -Stage "delay" `
      -Strategy $strategy -Batching $setting.Batching -SqlExecution "SqlBatch" -Rows $setting.Rows `
      -Writers 4 -BatchSize $setting.Batch -Capacity $setting.Capacity -Prefetch $setting.Prefetch `
      -SqlBatchCommands 4 -SqlBatchDelayMs $sqlBatchDelay
  }
}

# Every worker process owns its own coordinator and therefore one outstanding
# SqlBatch execution. This focused sweep shows when more processes restore useful
# database concurrency after writers have been coalesced.
foreach ($strategy in @("Individual", "MultipleInsertStatements")) {
  $setting = $settings[$strategy]
  foreach ($workers in @(2, 4)) {
    foreach ($execution in @("Native", "SqlBatch")) {
      $maximumCommands = if ($execution -eq "SqlBatch") { 4 } else { 1 }
      Add-Scenario -Name "sqlbatch-instances-$($strategy.ToLowerInvariant())-$($execution.ToLowerInvariant())-$workers" `
        -Stage "instance-scaling" -Strategy $strategy -Batching $setting.Batching `
        -SqlExecution $execution -Rows $setting.Rows -Workers $workers -Writers 4 -BatchSize $setting.Batch `
        -Capacity $setting.Capacity -Prefetch $setting.Prefetch -SqlBatchCommands $maximumCommands
    }
  }
}

# The data-shape check is deliberately limited to the two non-uniform profiles;
# Uniform is already covered by the writer sweep.
foreach ($entry in $settings.GetEnumerator()) {
  $setting = $entry.Value
  foreach ($distribution in @("ModerateSkew", "HotParent")) {
    foreach ($execution in @("Native", "SqlBatch")) {
      $maximumCommands = if ($execution -eq "SqlBatch") { 4 } else { 1 }
      Add-Scenario -Name "sqlbatch-distribution-$($entry.Key.ToLowerInvariant())-$($execution.ToLowerInvariant())-$($distribution.ToLowerInvariant())" `
        -Stage "distribution" -Strategy $entry.Key -Batching $setting.Batching -SqlExecution $execution `
        -Distribution $distribution `
        -Rows $setting.Rows -Writers 4 -BatchSize $setting.Batch -Capacity $setting.Capacity `
        -Prefetch $setting.Prefetch -SqlBatchCommands $maximumCommands
    }
  }
}

# Direct controls quantify the SqlBatch behavior without RabbitMQ delivery,
# deserialization, process batching, or acknowledgments.
foreach ($entry in $settings.GetEnumerator()) {
  $setting = $entry.Value
  foreach ($execution in @("Native", "SqlBatch")) {
    $maximumCommands = if ($execution -eq "SqlBatch") { 4 } else { 1 }
    Add-Scenario -Name "sqlbatch-direct-$($entry.Key.ToLowerInvariant())-$($execution.ToLowerInvariant())" `
      -Stage "direct-control" -Mode "DirectDatabase" -Strategy $entry.Key -Batching $setting.Batching `
      -SqlExecution $execution -Rows $setting.Rows -Writers 4 -BatchSize $setting.Batch `
      -Capacity $setting.Capacity -Prefetch $setting.Prefetch -SqlBatchCommands $maximumCommands
  }
}

$profile = [ordered]@{
  name = "SqlBatch"
  warmupRows = 500
  scenarios = $scenarios
}

$outputDirectory = Split-Path -Parent $OutputPath
New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
$profile | ConvertTo-Json -Depth 8 | Set-Content -Path $OutputPath -Encoding utf8NoBOM
Write-Host "Wrote $($scenarios.Count) scenarios to $OutputPath"
