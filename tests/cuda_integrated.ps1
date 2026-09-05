param([string]$ToolsDir, [string]$CudaDir, [string]$WorkDir)
$ErrorActionPreference = 'Continue'
if (-not $ToolsDir) { $ToolsDir = (Join-Path (Get-Location) 'build\tools') }
if (-not $CudaDir) { $CudaDir = (Join-Path (Get-Location) 'build\cuda') }
if (-not $WorkDir) { $WorkDir = (Join-Path (Get-Location) 'build\cudaint') }
if (-not (Test-Path $WorkDir)) { New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null }
Get-Process cfcoordinator,cfworker,cu_worker -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 100
Get-ChildItem $WorkDir -File | Remove-Item -Force -ErrorAction SilentlyContinue
$coord = Join-Path $ToolsDir 'cfcoordinator.exe'
$worker = Join-Path $ToolsDir 'cfworker.exe'
$cu = Join-Path $CudaDir 'cu_worker.exe'
$idfile = Join-Path $WorkDir 'domain.id'
$flowfile = Join-Path $WorkDir 'flow.id'
$flowfile2 = Join-Path $WorkDir 'flow2.id'
$state = Join-Path $WorkDir 'state.bin'
$status = Join-Path $WorkDir 'status.txt'
function Log($m) { Add-Content -Path $status -Value $m; Write-Output $m }
$port = 39071

$cp = Start-Process -FilePath $coord -ArgumentList @('--port',"$port",'--state',('"{0}"' -f $state)) -RedirectStandardError (Join-Path $WorkDir 'c.log') -RedirectStandardOutput (Join-Path $WorkDir 'c.out') -PassThru
$listening = $false
for ($i=0; $i -lt 100; $i++) { Start-Sleep -Milliseconds 50; if (Test-Path (Join-Path $WorkDir 'c.log')) { $c=Get-Content (Join-Path $WorkDir 'c.log') -Raw; if ($c -match 'listening'){ $listening=$true; break } } }
if (-not $listening) { Log "FAIL: coordinator not listening"; Stop-Process -Id $cp.Id -Force -ErrorAction SilentlyContinue; exit 1 }
Log "coordinator listening"

$logA = Join-Path $WorkDir 'workerA.log'
$procA = Start-Process -FilePath $cu -ArgumentList @('--coord','--port',"$port",'--scenario','work','--worker','1','--boot','1','--source','1','--sboot','1','--class','2','--idfile',('"{0}"' -f $idfile),'--flowfile',('"{0}"' -f $flowfile),'--stay') -RedirectStandardOutput $logA -RedirectStandardError (Join-Path $WorkDir 'workerA.err') -PassThru
$pidA = $procA.Id
$ready = $false
for ($i=0; $i -lt 200; $i++) { Start-Sleep -Milliseconds 50; if ($i -lt 1) { } ; if ((Test-Path $idfile) -and (Test-Path $flowfile)) { if ((Get-Content $logA -Raw -ErrorAction SilentlyContinue).Contains('CUDA_INTEGRATED ready')) { $ready=$true; break } } ; if ($procA.HasExited) { break } }
if (-not $ready) { Log "FAIL: Worker A not ready"; Stop-Process -Id $procA.Id -Force -ErrorAction SilentlyContinue; Stop-Process -Id $cp.Id -Force -ErrorAction SilentlyContinue; exit 1 }
$did = ((Get-Content $idfile -Raw).Trim().Split(' '))[0]
$flowId = ((Get-Content $flowfile -Raw).Trim().Split(' '))[0]
$flowGen = ((Get-Content $flowfile -Raw).Trim().Split(' '))[1]
Log "Worker A ready pid=$pidA did=$did flow=$flowId gen=$flowGen"

function QueryState($label) {
  $out = Join-Path $WorkDir ("q_" + $label + ".out")
  & $worker --port $port --scenario query --domain $did 2>&1 | Out-File $out
  $txt = Get-Content $out -Raw
  if ($txt -match 'state=(\d+)') { return [int]$Matches[1] }
  return 999
}

$before = QueryState 'before'
Log "before state=$before"

Stop-Process -Id $procA.Id -Force
Start-Sleep -Milliseconds 200
$after = QueryState 'after'
Log "after state=$after"

$repOut = Join-Path $WorkDir 'replay.out'
& $worker --port $port --scenario replay --worker 1 --boot 1 --source 1 --sboot 1 --domain $flowId --dgen $flowGen --offered 999999 --serviced 999999 --delta 1000 2>&1 | Out-File $repOut
$repTxt = Get-Content $repOut -Raw
$replayOk = ($repTxt -match 'all_rejected=1')
Log "replay all_rejected=$replayOk"

$logA2 = Join-Path $WorkDir 'workerAp.log'
$procA2 = Start-Process -FilePath $cu -ArgumentList @('--coord','--port',"$port",'--scenario','resurrect','--worker','1','--boot','3','--source','1','--sboot','3','--class','1','--idfile',('"{0}"' -f $idfile),'--flowfile',('"{0}"' -f $flowfile2),'--stay') -RedirectStandardOutput $logA2 -RedirectStandardError (Join-Path $WorkDir 'workerAp.err') -PassThru
$pidA2 = $procA2.Id
$primeReady = $false
for ($i=0; $i -lt 200; $i++) { Start-Sleep -Milliseconds 50; if ((Get-Content $logA2 -Raw -ErrorAction SilentlyContinue).Contains('CUDA_INTEGRATED prime-ready')) { $primeReady=$true; break } ; if ($procA2.HasExited) { break } }
Log "A' pid=$pidA2 primeReady=$primeReady"

$afterPrime = QueryState 'after_prime'
Log "afterPrime state=$afterPrime"

$ctlOut = Join-Path $WorkDir 'control.out'
& $worker --port $port --scenario control --domain $did --path $state 2>&1 | Out-File $ctlOut
$cp.WaitForExit()
Stop-Process -Id $procA2.Id -Force -ErrorAction SilentlyContinue  # teardown of the hold worker A'
$freshPid = ($pidA2 -ne $pidA)
$ok = ($before -ge 5) -and ($after -eq 12) -and ($replayOk) -and ($afterPrime -ne 12) -and ($freshPid) -and ($primeReady)
Log "RESULT ok=$ok before=$before after=$after afterPrime=$afterPrime replay=$replayOk freshPid=$freshPid primeReady=$primeReady"
if ($ok) { Log "CUDA_INTEGRATED PASS" } else { Log "CUDA_INTEGRATED FAIL" }
exit $(if ($ok) {0} else {1})
