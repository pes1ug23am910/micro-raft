[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$script:RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$script:ManifestPath = Join-Path $script:RepoRoot "Cargo.toml"
$script:NodeExe = Join-Path $script:RepoRoot "target\release\kv-node.exe"
$script:DemoData = Join-Path $script:RepoRoot "data\demo"
$script:RunDir = Join-Path $script:RepoRoot (".local\demo\{0}" -f (Get-Date -Format "yyyyMMdd-HHmmss-fff"))
$script:Processes = @{}
$script:LaunchCounts = @{}
$script:TranscriptStarted = $false

function Write-Step {
    param([Parameter(Mandatory = $true)][string]$Message)

    Write-Host ("[{0}] {1}" -f (Get-Date -Format "HH:mm:ss.fff"), $Message)
}

function Invoke-CurlText {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$Quiet
    )

    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    if ($Quiet) {
        $output = & curl.exe @Arguments 2>$null
    }
    else {
        $output = & curl.exe @Arguments
    }
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousPreference

    if ($exitCode -ne 0) {
        throw "curl.exe failed with exit code ${exitCode}: $($Arguments -join ' ')"
    }
    return ($output -join "`n")
}

function Get-NodeStatus {
    param(
        [Parameter(Mandatory = $true)][int]$NodeId,
        [switch]$Quiet
    )

    $port = 8100 + $NodeId
    try {
        $json = Invoke-CurlText -Arguments @(
            "--silent",
            "--show-error",
            "--fail",
            "--max-time", "1",
            "http://127.0.0.1:$port/status"
        ) -Quiet:$Quiet
        return ($json | ConvertFrom-Json)
    }
    catch {
        if ($Quiet) {
            return $null
        }
        throw
    }
}

function Wait-ForLeader {
    param(
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds,
        [int]$ExcludeNodeId = 0
    )

    $timer = [System.Diagnostics.Stopwatch]::StartNew()
    while ($timer.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        $leaders = @()
        foreach ($nodeId in 1..3) {
            if ($nodeId -eq $ExcludeNodeId) {
                continue
            }
            $status = Get-NodeStatus -NodeId $nodeId -Quiet
            if (($null -ne $status) -and ([string]$status.role -eq "leader")) {
                $leaders += $status
            }
        }
        if ($leaders.Count -eq 1) {
            return $leaders[0]
        }
        Start-Sleep -Milliseconds 100
    }
    throw "No single leader appeared within $TimeoutSeconds seconds. See $script:RunDir for node logs."
}

function Wait-ForAppliedIndex {
    param(
        [Parameter(Mandatory = $true)][int]$NodeId,
        [Parameter(Mandatory = $true)][long]$Index,
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds
    )

    $timer = [System.Diagnostics.Stopwatch]::StartNew()
    while ($timer.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        $status = Get-NodeStatus -NodeId $NodeId -Quiet
        if (($null -ne $status) -and ([long]$status.last_applied -ge $Index)) {
            return $status
        }
        Start-Sleep -Milliseconds 100
    }
    throw "Node $NodeId did not apply index $Index within $TimeoutSeconds seconds. See $script:RunDir for node logs."
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
        throw "Port $Port is already in use. Stop the process using it and rerun the demo."
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

    if (-not $script:LaunchCounts.ContainsKey($NodeId)) {
        $script:LaunchCounts[$NodeId] = 0
    }
    $script:LaunchCounts[$NodeId]++
    $launchNumber = $script:LaunchCounts[$NodeId]
    $stdout = Join-Path $script:RunDir ("node-{0}-run-{1}.stdout.log" -f $NodeId, $launchNumber)
    $stderr = Join-Path $script:RunDir ("node-{0}-run-{1}.stderr.log" -f $NodeId, $launchNumber)
    $arguments = @(
        "--id", [string]$NodeId,
        "--peers", ($peerSpecs -join ","),
        "--data-dir", ("data/demo/n{0}" -f $NodeId),
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
    Write-Step ("Started node {0} (PID {1})" -f $NodeId, $process.Id)
    return $process
}

function Stop-Node {
    param([Parameter(Mandatory = $true)][ValidateRange(1, 3)][int]$NodeId)

    if (-not $script:Processes.ContainsKey($NodeId)) {
        return
    }
    $process = $script:Processes[$NodeId]
    try {
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
            $null = $process.WaitForExit(5000)
        }
    }
    finally {
        $script:Processes.Remove($NodeId)
    }
}

function Write-Key {
    param(
        [Parameter(Mandatory = $true)][int]$NodeId,
        [Parameter(Mandatory = $true)][string]$Key,
        [Parameter(Mandatory = $true)][string]$Value
    )

    $port = 8100 + $NodeId
    $json = Invoke-CurlText -Arguments @(
        "--silent",
        "--show-error",
        "--fail",
        "--max-time", "5",
        "--request", "PUT",
        "--data-binary", $Value,
        "http://127.0.0.1:$port/kv/$Key"
    )
    $response = $json | ConvertFrom-Json
    if (($response.ok -ne $true) -or ($null -eq $response.index)) {
        throw "Unexpected PUT response for key '$Key': $json"
    }
    Write-Step ("Committed {0}={1} at log index {2}" -f $Key, $Value, $response.index)
    return $response
}

function Assert-Key {
    param(
        [Parameter(Mandatory = $true)][int]$NodeId,
        [Parameter(Mandatory = $true)][string]$Key,
        [Parameter(Mandatory = $true)][string]$ExpectedValue
    )

    $port = 8100 + $NodeId
    $actual = Invoke-CurlText -Arguments @(
        "--silent",
        "--show-error",
        "--fail",
        "--max-time", "2",
        "http://127.0.0.1:$port/kv/$Key"
    )
    if ($actual -cne $ExpectedValue) {
        throw "GET $Key from node $NodeId returned '$actual'; expected '$ExpectedValue'."
    }
    Write-Step ("Verified {0}={1} on node {2}" -f $Key, $ExpectedValue, $NodeId)
}

function Remove-DemoDataSafely {
    $dataRoot = [System.IO.Path]::GetFullPath((Join-Path $script:RepoRoot "data"))
    $expected = [System.IO.Path]::GetFullPath((Join-Path $dataRoot "demo"))
    $candidate = [System.IO.Path]::GetFullPath($script:DemoData)
    $comparison = [System.StringComparison]::OrdinalIgnoreCase
    $expectedPrefix = $dataRoot.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar

    if ((-not $candidate.Equals($expected, $comparison)) -or
        (-not $candidate.StartsWith($expectedPrefix, $comparison)) -or
        ([System.IO.Path]::GetFileName($candidate) -cne "demo")) {
        throw "Refusing to remove unexpected path: $candidate"
    }
    if (Test-Path -LiteralPath $candidate) {
        Write-Step "Removing previous data/demo directory"
        Remove-Item -LiteralPath $candidate -Recurse -Force
    }
    $null = New-Item -ItemType Directory -Path $candidate -Force
}

if ($PSVersionTable.PSVersion.Major -lt 5) {
    throw "scripts/demo.ps1 requires PowerShell 5.1 or newer."
}
if ($null -eq (Get-Command curl.exe -ErrorAction SilentlyContinue)) {
    throw "curl.exe is required but was not found on PATH."
}

$null = New-Item -ItemType Directory -Path $script:RunDir -Force
try {
    Start-Transcript -LiteralPath (Join-Path $script:RunDir "transcript.log") -Force | Out-Null
    $script:TranscriptStarted = $true

    Write-Step "Building the release binary"
    & cargo build --manifest-path $script:ManifestPath --release --locked -p kv-node
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed with exit code $LASTEXITCODE."
    }
    if (-not (Test-Path -LiteralPath $script:NodeExe -PathType Leaf)) {
        throw "Release binary was not created at $script:NodeExe."
    }

    foreach ($port in (7101..7103 + 8101..8103)) {
        Assert-PortAvailable -Port $port
    }
    Remove-DemoDataSafely

    foreach ($nodeId in 1..3) {
        $null = Start-Node -NodeId $nodeId
    }

    $leader = Wait-ForLeader -TimeoutSeconds 15
    $leaderId = [int]$leader.node_id
    Write-Step ("Initial leader is node {0} in term {1}" -f $leaderId, $leader.term)

    $durableValues = [ordered]@{
        language = "rust"
        consensus = "raft"
        durability = "fsync"
    }
    foreach ($entry in $durableValues.GetEnumerator()) {
        $null = Write-Key -NodeId $leaderId -Key $entry.Key -Value $entry.Value
    }

    Write-Step "Cluster status after the initial writes:"
    foreach ($nodeId in 1..3) {
        $status = Get-NodeStatus -NodeId $nodeId
        Write-Host ($status | ConvertTo-Json -Compress)
    }

    Write-Step ("Stopping leader node {0}" -f $leaderId)
    Stop-Node -NodeId $leaderId
    $failoverTimer = [System.Diagnostics.Stopwatch]::StartNew()
    $newLeader = Wait-ForLeader -TimeoutSeconds 15 -ExcludeNodeId $leaderId
    $failoverTimer.Stop()
    $newLeaderId = [int]$newLeader.node_id
    Write-Step ("Node {0} became leader after {1:N3} seconds" -f $newLeaderId, $failoverTimer.Elapsed.TotalSeconds)

    foreach ($entry in $durableValues.GetEnumerator()) {
        Assert-Key -NodeId $newLeaderId -Key $entry.Key -ExpectedValue $entry.Value
    }

    $afterFailover = Write-Key -NodeId $newLeaderId -Key "after-failover" -Value "survived"
    $targetIndex = [long]$afterFailover.index

    Write-Step ("Restarting old leader node {0} with its existing data" -f $leaderId)
    $null = Start-Node -NodeId $leaderId
    $caughtUp = Wait-ForAppliedIndex -NodeId $leaderId -Index $targetIndex -TimeoutSeconds 20
    Assert-Key -NodeId $leaderId -Key "after-failover" -ExpectedValue "survived"
    Write-Step ("Restarted node {0} caught up through index {1}" -f $leaderId, $caughtUp.last_applied)

    Write-Host ""
    Write-Host "SUCCESS: committed data survived leader failure, writes continued, and the restarted node caught up."
    Write-Host ("Failover time: {0:N3} seconds" -f $failoverTimer.Elapsed.TotalSeconds)
    Write-Host ("Detailed logs: {0}" -f $script:RunDir)
}
finally {
    foreach ($nodeId in @($script:Processes.Keys)) {
        Stop-Node -NodeId ([int]$nodeId)
    }
    if ($script:TranscriptStarted) {
        Stop-Transcript | Out-Null
    }
}
