param(
    [int]$Seconds = 60,
    [int]$ExpectedMinEventsPerSec = 50000,
    [string]$StatsUrl = "http://localhost:8080/stats/api"
)

function Get-TotalRecords {
    $data = Invoke-RestMethod -Uri $StatsUrl -ErrorAction Stop
    return ($data | Measure-Object -Property total -Sum).Sum
}

try {
    $before = Get-TotalRecords
    $startedAt = Get-Date

    Write-Host "Dang do server trong $Seconds giay..."
    Start-Sleep -Seconds $Seconds

    $after = Get-TotalRecords
    $finishedAt = Get-Date

    $elapsed = ($finishedAt - $startedAt).TotalSeconds
    $processed = $after - $before
    $rate = [math]::Round($processed / $elapsed, 2)

    Write-Host ""
    Write-Host "Processed records: $processed"
    Write-Host "Elapsed seconds:   $([math]::Round($elapsed, 2))"
    Write-Host "Throughput:        $rate event/giay"
    Write-Host "Expected minimum:  $ExpectedMinEventsPerSec event/giay"

    if ($rate -ge $ExpectedMinEventsPerSec) {
        Write-Host "PASS" -ForegroundColor Green
        exit 0
    }

    Write-Host "FAIL" -ForegroundColor Red
    exit 1
}
catch {
    Write-Host "Khong goi duoc server stats: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}