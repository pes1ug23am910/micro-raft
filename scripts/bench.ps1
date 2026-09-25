[CmdletBinding()]
param(
    [ValidateRange(1, 3600)]
    [int]$DurationSeconds = 30,

    [switch]$UseExistingCluster
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$script:RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$script:ManifestPath = Join-Path $script:RepoRoot "Cargo.toml"
$script:NodeExe = Join-Path $script:RepoRoot "target\release\kv-node.exe"
$script:RunDir = Join-Path $script:RepoRoot (".local\benchmarks\{0}" -f (Get-Date -Format "yyyyMMdd-HHmmss-fff"))
$script:Processes = @{}
$script:Jobs = @()
$script:OwnsCluster = $false
$script:TranscriptStarted = $false
# Git metadata is best effort; never bypass Git's repository-ownership checks.
$script:SourceRevision = $null
$script:SourceDirty = $null
if ($null -ne (Get-Command git -ErrorAction SilentlyContinue)) {
    # Windows PowerShell surfaces a native program's stderr as ErrorRecord
    # objects. Git metadata is optional, so warnings such as an unreadable
    # global excludes file must not abort the benchmark under Stop semantics.
    $savedErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $revision = @(& git -C $script:RepoRoot rev-parse --verify HEAD 2>$null)
        if (($LASTEXITCODE -eq 0) -and ($revision.Count -gt 0)) {
            $script:SourceRevision = [string]$revision[0]
            $status = @(& git -C $script:RepoRoot status --porcelain --untracked-files=no 2>$null)
            if ($LASTEXITCODE -eq 0) {
                $script:SourceDirty = [bool]$status
            }
        }
    }
    finally {
        $ErrorActionPreference = $savedErrorActionPreference
    }
}
$script:RustcVersion = (& rustc --version 2>$null | Select-Object -First 1)
$script:CargoVersion = (& cargo --version 2>$null | Select-Object -First 1)
$script:CargoLockSha256 = (Get-FileHash -LiteralPath (Join-Path $script:RepoRoot "Cargo.lock") -Algorithm SHA256).Hash.ToLowerInvariant()

function Write-Step {
    param([Parameter(Mandatory = $true)][string]$Message)

    Write-Host ("[{0}] {1}" -f (Get-Date -Format "HH:mm:ss.fff"), $Message)
}

function Get-NodeStatus {
    param([Parameter(Mandatory = $true)][int]$NodeId)

    $port = 8100 + $NodeId
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $json = & curl.exe `
        --silent `
        --show-error `
        --fail `
        --max-time 1 `
        "http://127.0.0.1:$port/status" 2>$null
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousPreference
    if ($exitCode -ne 0) {
        return $null
    }
    try {
        return (($json -join "`n") | ConvertFrom-Json)
    }
    catch {
        return $null
    }
}

function Get-RespondingStatuses {
    $statuses = @()
    foreach ($nodeId in 1..3) {
        $status = Get-NodeStatus -NodeId $nodeId
        if ($null -ne $status) {
            $statuses += $status
        }
    }
    return $statuses
}

function Assert-ExpectedCluster {
    param([Parameter(Mandatory = $true)][object[]]$Statuses)

    $nodeIds = @($Statuses | ForEach-Object { [int]$_.node_id } | Sort-Object -Unique)
    if (($nodeIds.Count -ne 3) -or (($nodeIds -join ",") -ne "1,2,3")) {
        throw "HTTP ports 8101-8103 did not identify themselves as nodes 1, 2, and 3."
    }
}

function Wait-ForLeader {
    param([Parameter(Mandatory = $true)][int]$TimeoutSeconds)

    $timer = [System.Diagnostics.Stopwatch]::StartNew()
    while ($timer.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        $leaders = @(Get-RespondingStatuses | Where-Object { [string]$_.role -eq "leader" })
        if ($leaders.Count -eq 1) {
            return $leaders[0]
        }
        Start-Sleep -Milliseconds 100
    }
    throw "No single leader appeared within $TimeoutSeconds seconds."
}

function Assert-PortAvailable {
    param([Parameter(Mandatory = $true)][int]$Port)

    $listener = New-Object System.Net.Sockets.TcpListener(
        [System.Net.IPAddress]::Loopback,
        $Port
    )
    try {
        $listener.Start()
    }
    catch {
        throw "Port $Port is already in use. Stop the process using it and rerun the benchmark."
    }
    finally {
        $listener.Stop()
    }
}

function Start-Node {
    param([Parameter(Mandatory = $true)][ValidateRange(1, 3)][int]$NodeId)

    $peerSpecs = @()
    foreach ($peerId in 1..3) {
        if ($peerId -ne $NodeId) {
            $peerSpecs += ("{0}@127.0.0.1:{1}" -f $peerId, (7100 + $peerId))
        }
    }

    $stdout = Join-Path $script:RunDir ("node-{0}.stdout.log" -f $NodeId)
    $stderr = Join-Path $script:RunDir ("node-{0}.stderr.log" -f $NodeId)
    $arguments = @(
        "--id", [string]$NodeId,
        "--peers", ($peerSpecs -join ","),
        "--data-dir", (".local/benchmarks/{0}/data/n{1}" -f (Split-Path $script:RunDir -Leaf), $NodeId),
        "--http-port", [string](8100 + $NodeId),
        "--raft-port", [string](7100 + $NodeId)
    )

    $process = Start-Process `
        -FilePath $script:NodeExe `
        -ArgumentList $arguments `
        -WorkingDirectory $script:RepoRoot `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -PassThru
    $script:Processes[$NodeId] = $process
    Write-Step ("Started benchmark node {0} (PID {1})" -f $NodeId, $process.Id)
}

function Stop-OwnedCluster {
    foreach ($nodeId in @($script:Processes.Keys)) {
        $process = $script:Processes[$nodeId]
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
            $null = $process.WaitForExit(5000)
        }
        $script:Processes.Remove($nodeId)
    }
}

function Get-Percentile {
    param(
        [Parameter(Mandatory = $true)][double[]]$SortedValues,
        [Parameter(Mandatory = $true)][ValidateRange(0.0, 100.0)][double]$Percentile
    )

    if ($SortedValues.Count -eq 0) {
        return [double]::NaN
    }
    $index = [Math]::Ceiling(($Percentile / 100.0) * $SortedValues.Count) - 1
    if ($index -lt 0) {
        $index = 0
    }
    return $SortedValues[$index]
}

$worker = {
    param(
        [int]$Port,
        [string]$TrialName,
        [int]$ClientId,
        [datetime]$StartAtUtc,
        [int]$RunSeconds
    )

    Add-Type -AssemblyName System.Net.Http
    $handler = New-Object System.Net.Http.HttpClientHandler
    $handler.UseProxy = $false
    $client = New-Object System.Net.Http.HttpClient($handler)
    $client.Timeout = [TimeSpan]::FromSeconds(5)
    $latencies = New-Object "System.Collections.Generic.List[double]"
    $failures = New-Object "System.Collections.Generic.List[string]"
    $sequence = 0

    try {
        while ([DateTime]::UtcNow -lt $StartAtUtc) {
            Start-Sleep -Milliseconds 10
        }
        $startedUtc = [DateTime]::UtcNow
        $deadlineUtc = $StartAtUtc.AddSeconds($RunSeconds)
        while ([DateTime]::UtcNow -lt $deadlineUtc) {
            $key = "bench-{0}-c{1}-n{2}" -f $TrialName, $ClientId, $sequence
            $uri = "http://127.0.0.1:{0}/kv/{1}" -f $Port, $key
            $request = New-Object System.Net.Http.HttpRequestMessage(
                [System.Net.Http.HttpMethod]::Put,
                $uri
            )
            $request.Content = New-Object System.Net.Http.StringContent(
                ("value-{0}" -f $sequence),
                [System.Text.Encoding]::UTF8,
                "text/plain"
            )
            $timer = [System.Diagnostics.Stopwatch]::StartNew()
            try {
                $response = $client.SendAsync($request).GetAwaiter().GetResult()
                $timer.Stop()
                if ([int]$response.StatusCode -eq 200) {
                    $latencies.Add($timer.Elapsed.TotalMilliseconds)
                }
                else {
                    $failures.Add(("HTTP {0}" -f [int]$response.StatusCode))
                }
                $response.Dispose()
            }
            catch {
                $timer.Stop()
                $failures.Add($_.Exception.GetType().FullName)
            }
            finally {
                $request.Dispose()
            }
            $sequence++
        }
        $finishedUtc = [DateTime]::UtcNow
        [pscustomobject]@{
            client_id = $ClientId
            started_utc = $startedUtc.ToString("o")
            finished_utc = $finishedUtc.ToString("o")
            attempts = $sequence
            successes = $latencies.Count
            failures = $failures.Count
            failure_kinds = @($failures | Group-Object | ForEach-Object {
                [pscustomobject]@{ kind = $_.Name; count = $_.Count }
            })
            latencies_ms = $latencies.ToArray()
        }
    }
    finally {
        $client.Dispose()
        $handler.Dispose()
    }
}

function Invoke-Trial {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][ValidateRange(1, 8)][int]$Clients,
        [Parameter(Mandatory = $true)][int]$LeaderPort
    )

    Write-Step ("Starting {0}: {1} client(s), {2} seconds" -f $Name, $Clients, $DurationSeconds)
    $startAt = [DateTime]::UtcNow.AddSeconds($(if ($Clients -eq 1) { 1 } else { 5 }))
    $results = @()

    if ($Clients -eq 1) {
        $results = @(& $worker $LeaderPort $Name 1 $startAt $DurationSeconds)
    }
    else {
        $script:Jobs = @()
        foreach ($clientId in 1..$Clients) {
            $script:Jobs += Start-Job -ScriptBlock $worker -ArgumentList @(
                $LeaderPort,
                $Name,
                $clientId,
                $startAt,
                $DurationSeconds
            )
        }
        $waited = @(Wait-Job -Job $script:Jobs -Timeout ($DurationSeconds + 60))
        if ($waited.Count -ne $script:Jobs.Count) {
            throw "One or more benchmark clients did not finish within the timeout."
        }
        foreach ($job in $script:Jobs) {
            $results += @(Receive-Job -Job $job)
        }
        Remove-Job -Job $script:Jobs -Force
        $script:Jobs = @()
    }

    $latencies = @()
    $successes = 0L
    $failures = 0L
    $attempts = 0L
    $started = @()
    $finished = @()
    foreach ($result in $results) {
        $latencies += @($result.latencies_ms | ForEach-Object { [double]$_ })
        $successes += [long]$result.successes
        $failures += [long]$result.failures
        $attempts += [long]$result.attempts
        $started += [DateTime]::Parse([string]$result.started_utc).ToUniversalTime()
        $finished += [DateTime]::Parse([string]$result.finished_utc).ToUniversalTime()
    }

    $sorted = @($latencies | Sort-Object)
    $firstStart = @($started | Sort-Object)[0]
    $lastFinish = @($finished | Sort-Object)[-1]
    $elapsedSeconds = ($lastFinish - $firstStart).TotalSeconds
    $writesPerSecond = if ($elapsedSeconds -gt 0) { $successes / $elapsedSeconds } else { 0.0 }
    $p50 = $null
    $p99 = $null
    if ($sorted.Count -gt 0) {
        $p50 = Get-Percentile -SortedValues $sorted -Percentile 50
        $p99 = Get-Percentile -SortedValues $sorted -Percentile 99
    }

    $raw = [ordered]@{
        trial = $Name
        clients = $Clients
        requested_duration_seconds = $DurationSeconds
        measured_duration_seconds = $elapsedSeconds
        leader_port = $LeaderPort
        attempts = $attempts
        successes = $successes
        failures = $failures
        writes_per_second = $writesPerSecond
        p50_ms = $p50
        p99_ms = $p99
        percentile_method = "nearest-rank"
        clients_raw = $results
    }
    $rawPath = Join-Path $script:RunDir ("{0}.json" -f $Name)
    $raw | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $rawPath -Encoding UTF8

    if ($sorted.Count -eq 0) {
        throw "$Name completed without a successful write. Raw results: $rawPath"
    }
    if ($failures -ne 0) {
        throw "$Name recorded $failures failed write(s); refusing to publish partial benchmark results. Raw results: $rawPath"
    }

    $summary = [pscustomobject]@{
        trial = $Name
        clients = $Clients
        writes_per_second = [Math]::Round($writesPerSecond, 2)
        p50_ms = [Math]::Round($p50, 3)
        p99_ms = [Math]::Round($p99, 3)
        successes = $successes
        failures = $failures
    }
    Write-Step ("{0}: {1:N2} writes/s, p50 {2:N3} ms, p99 {3:N3} ms, {4} failure(s)" -f `
        $Name, $writesPerSecond, $p50, $p99, $failures)
    return $summary
}

if ($PSVersionTable.PSVersion.Major -lt 5) {
    throw "scripts/bench.ps1 requires PowerShell 5.1 or newer."
}
if ($null -eq (Get-Command curl.exe -ErrorAction SilentlyContinue)) {
    throw "curl.exe is required but was not found on PATH."
}

$null = New-Item -ItemType Directory -Path $script:RunDir -Force
try {
    Start-Transcript -LiteralPath (Join-Path $script:RunDir "transcript.log") -Force | Out-Null
    $script:TranscriptStarted = $true

    $statuses = @(Get-RespondingStatuses)
    if (($statuses.Count -ne 0) -and ($statuses.Count -ne 3)) {
        throw ("Found {0} of 3 HTTP endpoints. Stop the partial cluster or start all three nodes." -f $statuses.Count)
    }

    if ($statuses.Count -eq 3) {
        if (-not $UseExistingCluster) {
            throw "A three-node cluster is already running. Stop it for an isolated benchmark, or pass -UseExistingCluster to write benchmark keys to it explicitly."
        }
        Assert-ExpectedCluster -Statuses $statuses
        Write-Step "Using the existing three-node cluster by explicit request"
    }
    else {
        Write-Step "No running cluster found; building and launching an isolated benchmark cluster"
        foreach ($port in (7101..7103 + 8101..8103)) {
            Assert-PortAvailable -Port $port
        }
        & cargo build --manifest-path $script:ManifestPath --release --locked -p kv-node
        if ($LASTEXITCODE -ne 0) {
            throw "cargo build failed with exit code $LASTEXITCODE."
        }
        if (-not (Test-Path -LiteralPath $script:NodeExe -PathType Leaf)) {
            throw "Release binary was not created at $script:NodeExe."
        }
        foreach ($nodeId in 1..3) {
            Start-Node -NodeId $nodeId
        }
        $script:OwnsCluster = $true
    }

    $summaries = @()
    foreach ($trial in @(
        @{ Name = "sequential-1"; Clients = 1 },
        @{ Name = "sequential-2"; Clients = 1 },
        @{ Name = "concurrent-1"; Clients = 8 },
        @{ Name = "concurrent-2"; Clients = 8 }
    )) {
        $leader = Wait-ForLeader -TimeoutSeconds 15
        $leaderPort = 8100 + [int]$leader.node_id
        $summaries += Invoke-Trial -Name $trial.Name -Clients $trial.Clients -LeaderPort $leaderPort
    }

    $summaryDocument = [ordered]@{
        generated_utc = [DateTime]::UtcNow.ToString("o")
        source_revision = $script:SourceRevision
        source_worktree_dirty = $script:SourceDirty
        rustc = [string]$script:RustcVersion
        cargo = [string]$script:CargoVersion
        cargo_lock_sha256 = $script:CargoLockSha256
        os = [System.Environment]::OSVersion.VersionString
        architecture = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
        duration_seconds_per_trial = $DurationSeconds
        payload = "text/plain value-<sequence>; unique key per write"
        fsync = "enabled for every accepted log append"
        percentile_method = "nearest-rank over successful end-to-end client latencies"
        trials = $summaries
    }
    $summaryDocument | ConvertTo-Json -Depth 5 | Set-Content `
        -LiteralPath (Join-Path $script:RunDir "summary.json") `
        -Encoding UTF8

    Write-Host ""
    $summaries | Format-Table -AutoSize
    Write-Host ("Raw benchmark output: {0}" -f $script:RunDir)
}
finally {
    foreach ($job in @($script:Jobs)) {
        Stop-Job -Job $job -ErrorAction SilentlyContinue
        Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
    }
    if ($script:OwnsCluster) {
        Stop-OwnedCluster
    }
    if ($script:TranscriptStarted) {
        Stop-Transcript | Out-Null
    }
}
