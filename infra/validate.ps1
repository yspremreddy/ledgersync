[CmdletBinding()]
param()

# Windows-native equivalent of validate.sh. It preserves containers, volumes,
# topics, connector offsets, and PostgreSQL data across repeated runs.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$ComposeFile = Join-Path $Root "compose.yaml"
$Report = Join-Path $Root (".local\infra-validation\{0}-{1}" -f (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmss"), $PID)
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)

$DockerCommand = Get-Command docker.exe -ErrorAction SilentlyContinue
if ($DockerCommand) {
    $DockerExe = $DockerCommand.Source
    $ComposeExe = $DockerExe
    $ComposePrefix = @("compose")
} else {
    $DockerExe = $null
    $ComposeExe = Join-Path $env:USERPROFILE ".docker\cli-plugins\docker-compose.exe"
    $ComposePrefix = @()
    if (-not (Test-Path -LiteralPath $ComposeExe)) {
        throw "Neither docker.exe nor the standalone Docker Compose plugin was found."
    }
}
$ComposeBase = @("--project-directory", $Root, "-f", $ComposeFile)

function Write-Utf8Lines {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [AllowEmptyCollection()][object[]]$Lines
    )
    $text = (($Lines | ForEach-Object { [string]$_ }) -join [Environment]::NewLine)
    if ($text.Length -gt 0) {
        $text += [Environment]::NewLine
    }
    [System.IO.File]::WriteAllText($Path, $text, $Utf8NoBom)
}

function Invoke-Compose {
    $commandArguments = @($args)
    & $ComposeExe @ComposePrefix @ComposeBase @commandArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Compose command failed with exit code $LASTEXITCODE`: $($commandArguments -join ' ')"
    }
}

function Invoke-KafkaTool {
    if ($args.Count -lt 1) {
        throw "Invoke-KafkaTool requires a tool name."
    }
    $tool = [string]$args[0]
    $toolArguments = if ($args.Count -gt 1) { @($args[1..($args.Count - 1)]) } else { @() }
    $commandArguments = @("exec", "-T", "-e", "KAFKA_HEAP_OPTS=-Xms16m -Xmx128m", "kafka", "/opt/kafka/bin/$tool") + $toolArguments
    Invoke-Compose @commandArguments
}

function Invoke-AppCheck {
    $commandArguments = @("--profile", "tools", "run", "--rm", "--no-deps", "-T", "app", "python", "/app/check_cdc.py") + @($args)
    Invoke-Compose @commandArguments
}

function Read-CgroupFile {
    param(
        [Parameter(Mandatory = $true)][string]$Service,
        [Parameter(Mandatory = $true)][string]$Path
    )
    return ((Invoke-Compose exec -T $Service cat $Path | Out-String).Trim())
}

function Capture-CgroupMemory {
    $lines = @("service raw_current_mib working_set_mib max_mib peak_mib max_events oom oom_kill")
    foreach ($service in @("postgres", "kafka", "connect")) {
        $current = [int64](Read-CgroupFile $service "/sys/fs/cgroup/memory.current")
        $maximum = [int64](Read-CgroupFile $service "/sys/fs/cgroup/memory.max")
        $peak = [int64](Read-CgroupFile $service "/sys/fs/cgroup/memory.peak")
        $events = Read-CgroupFile $service "/sys/fs/cgroup/memory.events"
        $stats = Read-CgroupFile $service "/sys/fs/cgroup/memory.stat"

        $eventValues = @{}
        foreach ($line in ($events -split "`r?`n")) {
            if ($line -match "^(\S+)\s+(\d+)$") {
                $eventValues[$matches[1]] = [int64]$matches[2]
            }
        }
        $inactiveFile = 0L
        foreach ($line in ($stats -split "`r?`n")) {
            if ($line -match "^inactive_file\s+(\d+)$") {
                $inactiveFile = [int64]$matches[1]
                break
            }
        }

        $rawMiB = [math]::Round($current / 1MB, 1)
        $workingMiB = [math]::Round(($current - $inactiveFile) / 1MB, 1)
        $maxMiB = [math]::Round($maximum / 1MB, 1)
        $peakMiB = [math]::Round($peak / 1MB, 1)
        $line = "$service $rawMiB $workingMiB $maxMiB $peakMiB $($eventValues['max']) $($eventValues['oom']) $($eventValues['oom_kill'])"
        $lines += $line
        Write-Output $line

        if ($eventValues["oom"] -ne 0 -or $eventValues["oom_kill"] -ne 0) {
            throw "$service reported cgroup OOM activity."
        }
    }
    Write-Utf8Lines (Join-Path $Report "memory-cgroup.txt") $lines
}

function Assert-FinalHealth {
    $ids = @()
    foreach ($service in @("postgres", "kafka", "connect")) {
        $id = ((Invoke-Compose ps -q $service | Out-String).Trim())
        if (-not $id) {
            throw "$service has no container."
        }
        $ids += $id
    }

    if ($DockerExe) {
        $states = @(& $DockerExe inspect --format "{{.Name}} status={{.State.Status}} health={{.State.Health.Status}} OOMKilled={{.State.OOMKilled}} restarts={{.RestartCount}}" @ids)
        if ($LASTEXITCODE -ne 0) {
            throw "docker inspect failed with exit code $LASTEXITCODE."
        }
        $states | ForEach-Object { Write-Output $_ }
        Write-Utf8Lines (Join-Path $Report "container-state.txt") $states
        foreach ($state in $states) {
            if ($state -notmatch "status=running health=healthy OOMKilled=false") {
                throw "Unexpected final container state: $state"
            }
        }

        $memory = @(& $DockerExe stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}" @ids)
        if ($LASTEXITCODE -ne 0) {
            throw "docker stats failed with exit code $LASTEXITCODE."
        }
        $memory | ForEach-Object { Write-Output $_ }
        Write-Utf8Lines (Join-Path $Report "memory.txt") $memory
    } else {
        $psRows = @((Invoke-Compose ps --format json) | ForEach-Object {
            if ([string]$_ -and ([string]$_).Trim()) {
                ([string]$_) | ConvertFrom-Json
            }
        })
        foreach ($service in @("postgres", "kafka", "connect")) {
            $row = @($psRows | Where-Object { $_.Service -eq $service })
            if ($row.Count -ne 1 -or $row[0].State -ne "running" -or $row[0].Health -ne "healthy") {
                throw "$service is not running and healthy."
            }
        }
        Write-Utf8Lines (Join-Path $Report "container-state.txt") @(
            $psRows | ForEach-Object { "$($_.Name) status=$($_.State) health=$($_.Health)" }
        )
    }
}

New-Item -ItemType Directory -Force -Path $Report | Out-Null
$TranscriptStarted = $false
$ExitCode = 0

try {
    Start-Transcript -Path (Join-Path $Report "validation.log") -Force | Out-Null
    $TranscriptStarted = $true

    Write-Output "== 1. Validate PowerShell, Compose, and Python tooling =="
    $tokens = $null
    $parseErrors = $null
    [System.Management.Automation.Language.Parser]::ParseFile($PSCommandPath, [ref]$tokens, [ref]$parseErrors) | Out-Null
    if ($parseErrors.Count -ne 0) {
        throw "PowerShell parser found $($parseErrors.Count) error(s) in $PSCommandPath."
    }
    Invoke-Compose version
    Invoke-Compose --profile tools config --quiet
    Invoke-Compose --profile tools build app
    Invoke-Compose --profile tools run --rm --no-deps -T app

    Write-Output "== 2. Start PostgreSQL and Kafka =="
    Invoke-Compose up -d --wait --wait-timeout 240 postgres kafka

    Write-Output "== 3. Verify databases and logical replication =="
    Invoke-Compose exec -T postgres psql -U ledgersync_admin -d ledger_source -v ON_ERROR_STOP=1 -c "SELECT current_database();"
    Invoke-Compose exec -T postgres psql -U ledgersync_admin -d ledgersync -v ON_ERROR_STOP=1 -c "SELECT current_database();"
    $walLevel = ((Invoke-Compose exec -T postgres psql -U ledgersync_admin -d postgres -Atqc "SHOW wal_level") | Out-String).Trim()
    if ($walLevel -ne "logical") {
        throw "Expected wal_level=logical; got $walLevel."
    }
    $smokeSql = Get-Content -LiteralPath (Join-Path $Root "infra\postgres\smoke.sql") -Raw
    Invoke-Compose exec -T postgres psql -U ledgersync_admin -d ledger_source -v ON_ERROR_STOP=1 -c $smokeSql
    $ledgerSql = Get-Content -LiteralPath (Join-Path $Root "infra\postgres\ledger.sql") -Raw
    Invoke-Compose exec -T postgres psql -U ledgersync_admin -d ledger_source -v ON_ERROR_STOP=1 -c $ledgerSql

    Write-Output "== 4. Provision explicit application and Connect topics =="
    foreach ($topic in @("ledgersync.gateway.v1", "ledgersync.ledger.outbox.v1", "ledgersync.settlement.v1", "ledgersync.refund.v1", "ledgersync.dlq.v1")) {
        Invoke-KafkaTool kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic $topic --partitions 3 --replication-factor 1 --config cleanup.policy=delete --config retention.ms=86400000 --config retention.bytes=16777216
    }
    foreach ($topic in @("ledgersync.connect.configs", "ledgersync.connect.offsets", "ledgersync.connect.status")) {
        Invoke-KafkaTool kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1 --config cleanup.policy=compact
    }
    Invoke-KafkaTool kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic ledgersync.cdc.public.cdc_smoke --partitions 1 --replication-factor 1 --config cleanup.policy=delete --config retention.ms=3600000 --config retention.bytes=16777216
    Invoke-KafkaTool kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic ledgersync.cdc.public.ledger_entries --partitions 1 --replication-factor 1 --config cleanup.policy=delete --config retention.ms=3600000 --config retention.bytes=16777216
    $topics = @(Invoke-KafkaTool kafka-topics.sh --bootstrap-server kafka:9092 --describe)
    $topics | ForEach-Object { Write-Output $_ }
    Write-Utf8Lines (Join-Path $Report "topics.txt") $topics

    Write-Output "== 5. Start Kafka Connect and register the connector =="
    Invoke-Compose up -d --wait --wait-timeout 240 connect
    Invoke-AppCheck configure

    $slotActive = "f"
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        $slotActive = ((Invoke-Compose exec -T postgres psql -U ledgersync_admin -d ledger_source -Atqc "SELECT active FROM pg_replication_slots WHERE slot_name='ledgersync_smoke_slot' AND database='ledger_source' AND plugin='pgoutput'") | Out-String).Trim()
        if ($slotActive -eq "t") {
            break
        }
        Start-Sleep -Seconds 2
    }
    if ($slotActive -ne "t") {
        throw "Replication slot did not become active."
    }

    Write-Output "== 6. Insert a unique smoke row =="
    $marker = "smoke-{0}-{1}" -f (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmss"), ([guid]::NewGuid().ToString("N").Substring(0, 12))
    Write-Utf8Lines (Join-Path $Report "marker.txt") @($marker)
    Write-Output $marker
    $insertSql = "INSERT INTO public.cdc_smoke (marker) VALUES ('$marker'); SELECT slot_name, active, wal_status, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_wal_bytes FROM pg_replication_slots WHERE slot_name='ledgersync_smoke_slot';"
    Invoke-Compose exec -T postgres psql -U ledgersync_admin -d ledger_source -v ON_ERROR_STOP=1 -c $insertSql

    Write-Output "== 7. Read Kafka and assert exactly one matching CDC insert =="
    $eventsPath = Join-Path $Report "cdc-events.jsonl"
    $consumerLog = Join-Path $Report "consumer.log"
    # Windows PowerShell 5.1 wraps native stderr as ErrorRecord objects when
    # ErrorActionPreference=Stop. The console consumer intentionally reports its
    # idle timeout on stderr, so temporarily make native stderr non-terminating.
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $events = @(& $ComposeExe @ComposePrefix @ComposeBase exec -T -e "KAFKA_HEAP_OPTS=-Xms16m -Xmx128m" kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic ledgersync.cdc.public.cdc_smoke --from-beginning --timeout-ms 30000 2> $consumerLog)
        $consumerStatus = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    Write-Utf8Lines $eventsPath $events
    if ($consumerStatus -ne 0) {
        $consumerText = Get-Content -LiteralPath $consumerLog -Raw
        if ($consumerStatus -ne 1 -or $consumerText -notmatch "org.apache.kafka.common.errors.TimeoutException") {
            throw "Kafka consumer failed with unexpected exit code $consumerStatus. See $consumerLog."
        }
    }
    Get-Content -LiteralPath $eventsPath | & $ComposeExe @ComposePrefix @ComposeBase --profile tools run --rm --no-deps -T app python /app/check_cdc.py event $marker
    if ($LASTEXITCODE -ne 0) {
        throw "CDC event assertion failed with exit code $LASTEXITCODE."
    }
    Invoke-AppCheck status

    Write-Output "== 8. Verify final health, memory, and OOM state =="
    Assert-FinalHealth
    Capture-CgroupMemory

    Write-Output "PASS: infrastructure validation and CDC smoke test completed."
} catch {
    $ExitCode = 1
    Write-Host "FAIL: $($_.Exception.Message)" -ForegroundColor Red
} finally {
    try {
        Write-Output "== Final service status =="
        Invoke-Compose ps -a
        $logs = @(Invoke-Compose logs --no-color --tail=150 postgres kafka connect)
        Write-Utf8Lines (Join-Path $Report "services.log") $logs
    } catch {
        if ($ExitCode -eq 0) {
            $ExitCode = 1
        }
        Write-Host "FAIL: could not capture final status/logs: $($_.Exception.Message)" -ForegroundColor Red
    }
    Write-Output "Evidence: $Report"
    Write-Output "Services remain running; no volumes or project data were deleted."
    if ($TranscriptStarted) {
        Stop-Transcript | Out-Null
    }
}

exit $ExitCode
