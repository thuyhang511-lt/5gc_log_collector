$prev = 0
$prevTime = Get-Date
while ($true) {
    try {
        $data = Invoke-RestMethod -Uri "http://localhost:8080/stats/api" -ErrorAction Stop
        $total = ($data | Measure-Object -Property total -Sum).Sum
        $now = Get-Date
        $elapsedSec = ($now - $prevTime).TotalSeconds
        $diff = $total - $prev
        $rate = [math]::Round($diff / $elapsedSec)
        Write-Host "server xu ly: $rate record/giay (tong: $total, khoang thoi gian: $([math]::Round($elapsedSec,2))s)"
        $prev = $total
        $prevTime = $now
    } catch {
        Write-Host "server chua san sang, thu lai..."
    }
    Start-Sleep -Seconds 1
}