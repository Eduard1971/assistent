param(
  [string]$PythonExe = "C:\Program Files\Python313\python.exe",
  [string]$ProjectDir = "C:\AI\salesperson\peter_assistant",
  [string]$TaskName = "Peter Assistant Gateway"
)
$ErrorActionPreference = "Stop"
$App = Join-Path $ProjectDir "app.py"
$LogDir = Join-Path $ProjectDir "logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$Action = New-ScheduledTaskAction -Execute $PythonExe -Argument ('"' + $App + '"') -WorkingDirectory $ProjectDir
$Trigger1 = New-ScheduledTaskTrigger -AtStartup
$Trigger2 = New-ScheduledTaskTrigger -AtLogOn
$Settings = New-ScheduledTaskSettingsSet -RestartCount 5 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit (New-TimeSpan -Days 3650) -MultipleInstances IgnoreNew -StartWhenAvailable
$Principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Highest
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger @($Trigger1,$Trigger2) -Settings $Settings -Principal $Principal -Force
Start-ScheduledTask -TaskName $TaskName
Write-Host "Installed and started: $TaskName"
