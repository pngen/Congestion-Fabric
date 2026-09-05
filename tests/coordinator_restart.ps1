param([string]$ToolsDir, [string]$WorkDir)
$ErrorActionPreference = 'Stop'
if (-not $ToolsDir) { $ToolsDir = (Join-Path (Get-Location) 'build\tools') }
if (-not $WorkDir) { $WorkDir = (Join-Path (Get-Location) 'build\restart') }
if (-not (Test-Path $WorkDir)) { New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null }
$coord = Join-Path $ToolsDir 'cfcoordinator.exe'
$worker = Join-Path $ToolsDir 'cfworker.exe'
$idfile = Join-Path $WorkDir 'domain.id'
$state = Join-Path $WorkDir 'state.bin'
foreach ($f in @($idfile,$state)) { if (Test-Path $f) { Remove-Item $f -Force } }
Get-Process cfcoordinator,cfworker -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 100

# ---- Phase 1: populate and save durable state ----
$p1 = 39061
$cp1 = Start-Process -FilePath $coord -ArgumentList @('--port',"$p1",'--state',('"{0}"' -f $state)) -RedirectStandardError (Join-Path $WorkDir 'c1.log') -RedirectStandardOutput (Join-Path $WorkDir 'c1.out') -PassThru
$listening = $false
for ($i=0; $i -lt 100; $i++) { Start-Sleep -Milliseconds 50; if (Test-Path (Join-Path $WorkDir 'c1.log')) { $c=Get-Content (Join-Path $WorkDir 'c1.log') -Raw; if ($c -match 'listening'){ $listening=$true; break } } }
if (-not $listening) { Write-Output "RESTART FAIL: coordinator1 did not listen"; Stop-Process -Id $cp1.Id -Force -ErrorAction SilentlyContinue; exit 1 }
& $worker --port $p1 --scenario drive --worker 1 --boot 1 --source 1 --sboot 1 --class 3 --bytes 52428800 --capacity 104857600 --offered 120000000 --serviced 45000000 --idfile $idfile 2>$null | Out-Null
& $worker --port $p1 --scenario control --domain (Get-Content $idfile) --path $state 2>$null | Out-Null
$cp1.WaitForExit()
Write-Output "RESTART: phase1 saved state, coordinator exited"

# ---- Phase 2: restart coordinator loading state ----
$p2 = 39062
$cp2 = Start-Process -FilePath $coord -ArgumentList @('--port',"$p2",'--state',('"{0}"' -f $state)) -RedirectStandardError (Join-Path $WorkDir 'c2.log') -RedirectStandardOutput (Join-Path $WorkDir 'c2.out') -PassThru
$listening = $false
for ($i=0; $i -lt 100; $i++) { Start-Sleep -Milliseconds 50; if (Test-Path (Join-Path $WorkDir 'c2.log')) { $c=Get-Content (Join-Path $WorkDir 'c2.log') -Raw; if ($c -match 'listening'){ $listening=$true; break } } }
if (-not $listening) { Write-Output "RESTART FAIL: coordinator2 did not listen"; Stop-Process -Id $cp2.Id -Force -ErrorAction SilentlyContinue; exit 1 }

function Query($label) {
  $out = Join-Path $WorkDir ("q_$label.out")
  & $worker --port $p2 --scenario query --domain (Get-Content $idfile) 2>$null | Out-File $out
  $txt = Get-Content $out -Raw
  if ($txt -match 'state=(\d+)') { return [int]$Matches[1] }
  return 999
}
$qAfterRestart = Query 'after_restart'
# republish fresh evidence at a lower load (clears revalidation)
& $worker --port $p2 --scenario resurrect --worker 1 --boot 3 --source 1 --sboot 3 --class 1 --bytes 10485760 --offered 20000000 --serviced 18000000 --idfile $idfile 2>$null | Out-Null
$qAfterFresh = Query 'after_fresh'

& $worker --port $p2 --scenario shutdown 2>$null | Out-Null
$cp2.WaitForExit()
Write-Output "RESTART: q_after_restart=$qAfterRestart q_after_fresh=$qAfterFresh"
$ok = ($qAfterRestart -eq 12) -and ($qAfterFresh -ne 12)
Write-Output ("RESTART " + ($(if ($ok) {"PASS"} else {"FAIL"})))
exit $(if ($ok) {0} else {1})
