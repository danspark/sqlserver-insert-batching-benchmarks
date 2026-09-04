[CmdletBinding()]
param(
  [string] $OutputPath = (Join-Path $PSScriptRoot "../config/Confirmation.json")
)

$ErrorActionPreference = "Stop"
$seed = 1592606758
$rows = 750000
$scenarios = [System.Collections.Generic.List[object]]::new()
$settings = @(
  @{ Name = "individual"; Strategy = "Individual"; Batching = "None"; Workers = 2; Writers = 4; Batch = 1; Capacity = 1000; Prefetch = 250 },
  @{ Name = "table-valued-parameter"; Strategy = "TableValuedParameter"; Batching = "Channel"; Workers = 1; Writers = 2; Batch = 5000; Capacity = 10000; Prefetch = 20000 },
  @{ Name = "multiple-statements"; Strategy = "MultipleInsertStatements"; Batching = "Channel"; Workers = 4; Writers = 2; Batch = 50; Capacity = 2000; Prefetch = 200 },
  @{ Name = "multi-row-values"; Strategy = "MultiRowValues"; Batching = "Channel"; Workers = 2; Writers = 2; Batch = 100; Capacity = 2000; Prefetch = 400 },
  @{ Name = "bulk-copy"; Strategy = "BulkCopy"; Batching = "Channel"; Workers = 1; Writers = 2; Batch = 1000; Capacity = 8000; Prefetch = 2000 }
)

foreach ($setting in $settings) {
  foreach ($repetition in 1..3) {
    $scenarios.Add([ordered]@{
      name = "confirmed-$($setting.Name)"
      stage = "confirmation"
      mode = "Queue"
      strategy = $setting.Strategy
      batching = $setting.Batching
      distribution = "Uniform"
      seed = $seed
      rowCount = $rows
      workerInstances = $setting.Workers
      writersPerInstance = $setting.Writers
      batchSize = $setting.Batch
      maximumBatchingDelayMilliseconds = 5
      channelCapacity = $setting.Capacity
      rabbitMqPrefetch = $setting.Prefetch
      repetition = $repetition
    })
  }
}

$profile = [ordered]@{
  name = "Confirmation"
  warmupRows = 5000
  scenarios = $scenarios
}

$outputDirectory = Split-Path -Parent $OutputPath
New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
$profile | ConvertTo-Json -Depth 8 | Set-Content -Path $OutputPath -Encoding utf8NoBOM
Write-Host "Wrote $($scenarios.Count) scenarios to $OutputPath"
