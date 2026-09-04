[CmdletBinding()]
param(
  [string] $OutputPath = (Join-Path $PSScriptRoot "../config/SqlBatchConfirmation.json")
)

$ErrorActionPreference = "Stop"
$seed = 1592606758
$scenarios = [System.Collections.Generic.List[object]]::new()
$settings = @(
  @{
    Name = "individual-native"; Strategy = "Individual"; Execution = "Native"; Rows = 50000
    Batching = "None"; Writers = 4; Batch = 1; Capacity = 1000; Prefetch = 250
    Commands = 1; RequestConcurrency = 1
  },
  @{
    Name = "individual-sqlbatch"; Strategy = "Individual"; Execution = "SqlBatch"; Rows = 50000
    Batching = "None"; Writers = 4; Batch = 1; Capacity = 1000; Prefetch = 250
    Commands = 1; RequestConcurrency = 4
  },
  @{
    Name = "tvp-native"; Strategy = "TableValuedParameter"; Execution = "Native"; Rows = 750000
    Batching = "Channel"; Writers = 4; Batch = 500; Capacity = 4000; Prefetch = 2000
    Commands = 1; RequestConcurrency = 1
  },
  @{
    Name = "tvp-sqlbatch"; Strategy = "TableValuedParameter"; Execution = "SqlBatch"; Rows = 750000
    Batching = "Channel"; Writers = 4; Batch = 500; Capacity = 4000; Prefetch = 2000
    Commands = 4; RequestConcurrency = 1
  },
  @{
    Name = "multiple-statements-native"; Strategy = "MultipleInsertStatements"; Execution = "Native"; Rows = 500000
    Batching = "Channel"; Writers = 4; Batch = 50; Capacity = 2000; Prefetch = 400
    Commands = 1; RequestConcurrency = 1
  },
  @{
    Name = "multiple-statements-sqlbatch"; Strategy = "MultipleInsertStatements"; Execution = "SqlBatch"; Rows = 500000
    Batching = "Channel"; Writers = 4; Batch = 50; Capacity = 2000; Prefetch = 400
    Commands = 1; RequestConcurrency = 4
  },
  @{
    Name = "multi-row-values-native"; Strategy = "MultiRowValues"; Execution = "Native"; Rows = 500000
    Batching = "Channel"; Writers = 4; Batch = 100; Capacity = 2000; Prefetch = 800
    Commands = 1; RequestConcurrency = 1
  },
  @{
    Name = "multi-row-values-sqlbatch"; Strategy = "MultiRowValues"; Execution = "SqlBatch"; Rows = 500000
    Batching = "Channel"; Writers = 4; Batch = 100; Capacity = 2000; Prefetch = 800
    Commands = 1; RequestConcurrency = 4
  }
)

foreach ($setting in $settings) {
  foreach ($repetition in 1..3) {
    $scenarios.Add([ordered]@{
      name = "confirmed-sqlbatch-$($setting.Name)"
      stage = "confirmation"
      mode = "Queue"
      strategy = $setting.Strategy
      batching = $setting.Batching
      sqlExecution = $setting.Execution
      distribution = "Uniform"
      seed = $seed
      rowCount = $setting.Rows
      workerInstances = 1
      writersPerInstance = $setting.Writers
      batchSize = $setting.Batch
      maximumBatchingDelayMilliseconds = 5
      channelCapacity = $setting.Capacity
      rabbitMqPrefetch = $setting.Prefetch
      sqlBatchMaximumCommands = $setting.Commands
      sqlBatchMaximumDelayMilliseconds = 1
      sqlBatchRequestConcurrency = $setting.RequestConcurrency
      repetition = $repetition
    })
  }
}

$profile = [ordered]@{
  name = "SqlBatchConfirmation"
  warmupRows = 5000
  scenarios = $scenarios
}

$outputDirectory = Split-Path -Parent $OutputPath
New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
$profile | ConvertTo-Json -Depth 8 | Set-Content -Path $OutputPath -Encoding utf8NoBOM
Write-Host "Wrote $($scenarios.Count) scenarios to $OutputPath"
