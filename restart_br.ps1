Get-WmiObject Win32_Process -Filter "Name='python.exe'" | ForEach-Object {
    $p = Get-Process -Id $_.ProcessId -ErrorAction SilentlyContinue
    if ($p -and $p.Path -like '*Python*') {
        $cl = $_.CommandLine
        if ($cl -like '*Main.py*') { Write-Output "killing $($_.ProcessId)"; Stop-Process -Id $_.ProcessId -Force }
    }
}
Start-Sleep 2
Write-Output "old bot stopped"
