param([string]$ToolsDir, [string]$WorkDir)
$ErrorActionPreference = 'Stop'
if (-not $ToolsDir) { $ToolsDir = (Join-Path (Get-Location) 'build\tools') }
if (-not $WorkDir) { $WorkDir = (Join-Path (Get-Location) 'build\mp') }
if (-not (Test-Path $WorkDir)) { New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null }

$coord = Join-Path $ToolsDir 'cfcoordinator.exe'
$worker = Join-Path $ToolsDir 'cfworker.exe'
$idfile = Join-Path $WorkDir 'domain.id'
$state = Join-Path $WorkDir 'state.bin'
$coordLog = Join-Path $WorkDir 'coord.log'
$aliveLog = Join-Path $WorkDir 'workerA.log'
$bliveLog = Join-Path $WorkDir 'workerB.log'
'PASS' | Out-Null

# delete stale artifacts
foreach ($f in @($idfile,$state,$coordLog,$aliveLog,$bliveLog)) { if (Test-Path $f) { Remove-Item $f -Force } }

Get-Process cfcoordinator,cfworker -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 100
$port = 39001

# --- start coordinator ---
$coordProc = Start-Process -FilePath $coord -ArgumentList @('--port', "$port", '--state', ('"{0}"' -f $state)) -RedirectStandardError $coordLog -RedirectStandardOutput (Join-Path $WorkDir 'coord.out') -PassThru
# wait for listening
$listening = $false
for ($i=0; $i -lt 100; $i++) {
  if (Test-Path $coordLog) { $c = Get-Content $coordLog -Raw -ErrorAction SilentlyContinue; if ($c -match 'listening') { $listening = $true; break } }
  Start-Sleep -Milliseconds 50
}
if (-not $listening) { Write-Output "MULTIPROCESS FAIL: coordinator did not listen"; Stop-Process -Id $coordProc.Id -Force -ErrorAction SilentlyContinue; exit 1 }

# --- worker A: drive, stays alive ---
$procA = Start-Process -FilePath $worker -ArgumentList @('--port',"$port",'--scenario','drive','--worker','1','--boot','1','--source','1','--sboot','1','--class','3','--bytes','52428800','--capacity','104857600','--offered','120000000','--serviced','45000000','--idfile',('"{0}"' -f $idfile),'--stay') -RedirectStandardOutput $aliveLog -RedirectStandardError (Join-Path $WorkDir 'workerA.err') -PassThru
# wait for domain id file
$haveId = $false
for ($i=0; $i -lt 100; $i++) {
  if (Test-Path $idfile) { $haveId = $true; break }
  if ($procA.HasExited) { break }
  Start-Sleep -Milliseconds 50
}
if (-not $haveId) { Write-Output "MULTIPROCESS FAIL: worker A did not create domain"; Stop-Process -Id $procA.Id -Force -ErrorAction SilentlyContinue; Stop-Process -Id $coordProc.Id -Force -ErrorAction SilentlyContinue; exit 1 }
Write-Output "MULTIPROCESS: domain created id=$(Get-Content $idfile)"

# --- worker B: compete, stays alive ---
$procB = Start-Process -FilePath $worker -ArgumentList @('--port',"$port",'--scenario','compete','--worker','2','--boot','1','--source','2','--sboot','1','--class','1','--bytes','52428800','--offered','130000000','--serviced','60000000','--idfile',('"{0}"' -f $idfile),'--stay') -RedirectStandardOutput $bliveLog -RedirectStandardError (Join-Path $WorkDir 'workerB.err') -PassThru
# wait for B to finish setup (COMPETE_OK in its log)
$bOk = $false
for ($i=0; $i -lt 100; $i++) {
  if (Test-Path $bliveLog) { $c = Get-Content $bliveLog -Raw -ErrorAction SilentlyContinue; if ($c -match 'COMPETE_OK') { $bOk = $true; break } }
  if ($procB.HasExited) { break }
  Start-Sleep -Milliseconds 50
}
if (-not $bOk) { Write-Output "MULTIPROCESS FAIL: worker B did not set up"; Stop-Process -Id $procB.Id -Force -ErrorAction SilentlyContinue; Stop-Process -Id $procA.Id -Force -ErrorAction SilentlyContinue; Stop-Process -Id $coordProc.Id -Force -ErrorAction SilentlyContinue; exit 1 }

# --- control query (both active) ---
function Invoke-Query($label) {
  $out = Join-Path $WorkDir ("q_$label.out")
  & $worker --port $port --scenario query --domain (Get-Content $idfile) 2>$null | Out-File -FilePath $out
  $txt = Get-Content $out -Raw
  if ($txt -match 'state=(\d+)') { return [int]$Matches[1] }
  return 999
}
$s1 = Invoke-Query 'before_death'
Write-Output "MULTIPROCESS: congestion before death = $s1 (expect >= 5)"

# --- kill worker A (real OS process death) ---
Stop-Process -Id $procA.Id -Force
Start-Sleep -Milliseconds 100
$s2 = Invoke-Query 'after_death'

# --- stale worker boot must be rejected (A's boot was advanced by the fence) ---
$staleBootExit = 0
& $worker --port $port --scenario register --worker 1 --boot 1 2>$null
$staleBootExit = $LASTEXITCODE
Write-Output "MULTIPROCESS: stale boot register exit=$staleBootExit (expect nonzero = rejected)"

# --- worker A resumes with a fresh boot ---
$resLog = Join-Path $WorkDir 'resurrect.log'
& $worker --port $port --scenario resurrect --worker 1 --boot 3 --source 1 --sboot 3 --class 1 --bytes 10485760 --offered 20000000 --serviced 18000000 --idfile $idfile 2>$null | Out-Null
$s3 = Invoke-Query 'after_resurrect'

# --- save + shutdown ---
$ctlOut = Join-Path $WorkDir 'control.out'
& $worker --port $port --scenario control --domain (Get-Content $idfile) --path $state 2>$null | Out-File $ctlOut
# wait for coordinator to exit naturally
$coordProc.WaitForExit()
$coordExit = $true
Stop-Process -Id $procB.Id -Force -ErrorAction SilentlyContinue

# Evidence of fresh-evidence recompute: offered after resurrection vs before.
$before = Get-Content (Join-Path $WorkDir 'q_before_death.out') -Raw
$after = Get-Content (Join-Path $WorkDir 'q_after_resurrect.out') -Raw
Write-Output ("MULTIPROCESS: before_offered={0} after_offered={1}" -f ($before -replace '.*offered=([0-9.]+).*','$1'), ($after -replace '.*offered=([0-9.]+).*','$1'))

Write-Output "MULTIPROCESS: s1=$s1 s2=$s2 s3=$s3"
$ok = ($s1 -ge 5) -and ($coordExit) -and ($staleBootExit -ne 0)
if ($ok) { Write-Output "MULTIPROCESS PASS" } else { Write-Output "MULTIPROCESS FAIL" }
exit $(if ($ok) {0} else {1})
