$r = ([wmiclass]'Win32_Process').Create('cmd /c "python -u Main.py > bot_out.log 2> bot_err.log"', 'C:\Users\W8SOJIB\Downloads\BR\BR')
Write-Output ("ReturnValue=" + $r.ReturnValue + " PID=" + $r.ProcessId)
