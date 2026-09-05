// Congestion Fabric -- real CUDA worker proof (scenario E).
// A standalone OS process that owns real CUDA allocations and transfer work.
// In --hold mode it stays alive (so the orchestrator can kill it as a real
// process); in --revalidate mode it re-discovers the RTX 5090, re-runs a real
// transfer/kernel path, checks CPU parity and returns device memory to baseline.
#include <cuda_runtime.h>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

static int g_fails = 0;
static void chk(bool c, const char* m) { if (!c) { std::printf("FAIL: %s\n", m); ++g_fails; } }
static double now_s() {
  using namespace std::chrono;
  return duration_cast<duration<double>>(steady_clock::now().time_since_epoch()).count();
}

__global__ void fill_kernel(char* dst, int n) {
  int i = blockIdx.x * blockDim.x + threadIdx.x;
  if (i < n) dst[i] = 'r';
}

static bool do_cuda_work(const char* tag) {
  int dev = 0;
  cudaError_t e = cudaGetDeviceCount(&dev);
  if (e != cudaSuccess || dev < 1) { std::printf("ERROR: no CUDA device\n"); return false; }
  cudaDeviceProp prop{};
  cudaGetDeviceProperties(&prop, 0);
  std::printf("%s: device=%s cc=%d.%d\n", tag, prop.name, prop.major, prop.minor);

  const std::size_t N = 16u << 20;
  char* d = nullptr;
  e = cudaMalloc(&d, N);
  if (e != cudaSuccess) { std::printf("ERROR: cudaMalloc\n"); return false; }
  fill_kernel<<<(N / 256), 256>>>(d, static_cast<int>(N));
  e = cudaDeviceSynchronize();
  if (e != cudaSuccess) { std::printf("ERROR: kernel sync\n"); cudaFree(d); return false; }

  std::vector<char> host(N, 0);
  double t0 = now_s();
  e = cudaMemcpyAsync(host.data(), d, N, cudaMemcpyDeviceToHost, 0);
  cudaDeviceSynchronize();
  if (e != cudaSuccess) { std::printf("ERROR: D2H\n"); cudaFree(d); return false; }
  double dt = now_s() - t0;
  bool parity = (host[0] == 'r') && (host[N - 1] == 'r');
  chk(parity, "CPU parity after real CUDA kernel+transfer");
  std::printf("%s: completed=%llu bytes %.6f s -> %.3f MB/s parity=%d\n", tag,
              (unsigned long long)N, dt, static_cast<double>(N) / dt / (1024.0 * 1024.0),
              parity ? 1 : 0);
  cudaFree(d);
  return parity && (cudaGetLastError() == cudaSuccess);
}

int main(int argc, char** argv) {
  bool hold = false;
  for (int i = 1; i < argc; ++i) if (!std::strcmp(argv[i], "--hold")) hold = true;
  bool ok = do_cuda_work(hold ? "CUDA_WORKER[hold]" : "CUDA_WORKER[revalidate]");
  if (ok) std::printf("CUDA_WORKER_READY mode=%s\n", hold ? "hold" : "revalidate");
  if (hold) {
    // Stay alive so the orchestrator can kill this process as a real OS process.
    std::printf("CUDA_WORKER_HOLD_MODE\n");
    std::fflush(stdout);
    while (true) std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  cudaDeviceReset();
  std::printf("CUDA_WORKER_DONE ok=%d\n", ok ? 1 : 0);
  return ok ? 0 : 1;
}
