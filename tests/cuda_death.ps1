param([string]$CudaDir, [string]$WorkDir)
$ErrorActionPreference = 'Stop'
if (-not $CudaDir) { $CudaDir = (Join-Path (Get-Location) 'build\cuda') }
if (-not $WorkDir) { $WorkDir = (Join-Path (Get-Location) 'build\cudadeath') }
if (-not (Test-Path $WorkDir)) { New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null }
Get-Process cu_worker -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Remove-Item "$WorkDir\*" -Force -ErrorAction SilentlyContinue
$cu = Join-Path $CudaDir 'cu_worker.exe'
$holdLog = Join-Path $WorkDir 'hold.log'
# ---- real CUDA worker process, hold mode ----
$p = Start-Process -FilePath $cu -ArgumentList '--hold' -RedirectStandardOutput $holdLog -RedirectStandardError (Join-Path $WorkDir 'hold.err') -PassThru
$ready = $false
for ($i=0; $i -lt 150; $i++) { Start-Sleep -Milliseconds 50; if (Test-Path $holdLog) { $c=Get-Content $holdLog -Raw -ErrorAction SilentlyContinue; if ($c -match 'CUDA_WORKER_READY') { $ready=$true; break } } }
if (-not $ready) { Write-Output "CUDA_DEATH FAIL: worker did not become ready"; Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue; exit 1 }
Write-Output "CUDA_DEATH: real worker ready; killing it"
# ---- kill the worker as a real OS process ----
Stop-Process -Id $p.Id -Force
Start-Sleep -Milliseconds 150
# ---- fresh worker A' re-discovers the RTX 5090, does real work, cleans up ----
$revLog = Join-Path $WorkDir 'revalidate.log'
& $cu 2>&1 | Out-File $revLog
$exit = $LASTEXITCODE
$txt = Get-Content $revLog -Raw
$ok = ($exit -eq 0) -and ($txt -match 'CUDA_WORKER_DONE ok=1')
if ($txt -match 'CUDA_WORKER_DONE ok=(\d+)') { $doneOk = $Matches[1] } else { $doneOk = '?' }
Write-Output ("CUDA_DEATH: revalidate exit=$exit doneOk=$doneOk")
if ($ok) { Write-Output "CUDA_DEATH PASS" } else { Write-Output "CUDA_DEATH FAIL" }
exit $(if ($ok) {0} else {1})
