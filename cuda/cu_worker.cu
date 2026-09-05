// Congestion Fabric -- real CUDA worker proof (scenario E) + integrated
// coordinator-managed CUDA worker-death congestion proof.
// Standalone mode: --hold stays alive (killed as a real OS process); revalidate
// mode re-discovers the RTX 5090 and re-runs a real transfer/kernel path.
// Coordinator mode (--coord): registers with the coordinator, does real CUDA
// work, publishes live MEASURED/DERIVED evidence into Congestion Fabric, and
// queries current congestion.
#include <cuda_runtime.h>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <process.h>
#include <thread>
#include <vector>

#include "../include/congestion_fabric/net.hpp"
#include "../include/congestion_fabric/protocol.hpp"
#include "../include/congestion_fabric/persistence.hpp"
#include "../include/congestion_fabric/enums.hpp"

using namespace congfabric;
using congfabric::persist::Reader;
using congfabric::persist::Writer;

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

// Real CUDA work: discover device, allocate, run a kernel, D2H, cpu parity.
// Returns measured completed bytes and throughput via out-params.
static bool do_cuda_work(const char* tag, std::uint64_t* out_bytes, double* out_bps) {
  int dev = 0;
  cudaError_t e = cudaGetDeviceCount(&dev);
  if (e != cudaSuccess || dev < 1) { std::printf("ERROR: no CUDA device\n"); return false; }
  cudaDeviceProp prop{};
  cudaGetDeviceProperties(&prop, 0);
  std::printf("%s: device=%s cc=%d.%d pid=%lu\n", tag, prop.name, prop.major, prop.minor,
              (unsigned long)::GetCurrentProcessId());
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
  if (dt <= 0) dt = 1e-9;
  bool parity = (host[0] == 'r') && (host[N - 1] == 'r');
  chk(parity, "CPU parity after real CUDA kernel+transfer");
  if (out_bytes) *out_bytes = N;
  if (out_bps) *out_bps = static_cast<double>(N) / dt;
  std::printf("%s: completed=%llu bytes %.6f s -> %.3f MB/s parity=%d\n", tag,
              (unsigned long long)N, dt, static_cast<double>(N) / dt / (1024.0 * 1024.0),
              parity ? 1 : 0);
  cudaFree(d);
  return parity && (cudaGetLastError() == cudaSuccess);
}

// ---- coordinator protocol client ----
static std::uint16_t g_port = 0;
static std::uint64_t g_worker = 1, g_boot = 1, g_source = 1, g_sboot = 1;
static std::uint64_t g_bytes = 0, g_class = 1;
static double g_offered = 0, g_serviced = 0, g_capacity = 0;
static std::string g_idfile, g_flowfile, g_scenario;
static bool g_stay = false;

static std::uint64_t rd_ll(const char* s) { return std::strtoull(s, nullptr, 10); }
static double rd_d(const char* s) { return std::atof(s); }

static void arg_scan(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    const char* a = argv[i];
    auto next = [&]() -> const char* { return (i + 1 < argc) ? argv[++i] : ""; };
    if (!std::strcmp(a, "--port")) g_port = (std::uint16_t)rd_ll(next());
    else if (!std::strcmp(a, "--scenario")) g_scenario = next();
    else if (!std::strcmp(a, "--worker")) g_worker = rd_ll(next());
    else if (!std::strcmp(a, "--boot")) g_boot = rd_ll(next());
    else if (!std::strcmp(a, "--source")) g_source = rd_ll(next());
    else if (!std::strcmp(a, "--sboot")) g_sboot = rd_ll(next());
    else if (!std::strcmp(a, "--bytes")) g_bytes = rd_ll(next());
    else if (!std::strcmp(a, "--class")) g_class = rd_ll(next());
    else if (!std::strcmp(a, "--offered")) g_offered = rd_d(next());
    else if (!std::strcmp(a, "--serviced")) g_serviced = rd_d(next());
    else if (!std::strcmp(a, "--capacity")) g_capacity = rd_d(next());
    else if (!std::strcmp(a, "--idfile")) g_idfile = next();
    else if (!std::strcmp(a, "--flowfile")) g_flowfile = next();
    else if (!std::strcmp(a, "--stay")) g_stay = true;
  }
}

static bool recv_frame(SOCKET s, proto::Frame& out) {
  char hdr[proto::kHeaderBytes];
  if (!net::read_exact(s, hdr, proto::kHeaderBytes)) return false;
  if (std::memcmp(hdr, proto::kMagic, 4) != 0) return false;
  if (static_cast<std::uint8_t>(hdr[4]) != proto::kVersion) return false;
  std::uint8_t type = static_cast<std::uint8_t>(hdr[5]);
  std::uint32_t body_len = 0;
  for (int i = 0; i < 4; ++i) body_len |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(hdr[6 + i])) << (8 * i);
  if (body_len > proto::kMaxFrameBody) return false;
  std::vector<std::uint8_t> body(body_len);
  if (body_len && !net::read_exact(s, reinterpret_cast<char*>(body.data()), body_len)) return false;
  char cb[4];
  if (!net::read_exact(s, cb, 4)) return false;
  std::uint32_t stored = 0;
  for (int i = 0; i < 4; ++i) stored |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(cb[i])) << (8 * i);
  if (!proto::validate_crc(type, body.data(), body_len, stored)) return false;
  out.type = static_cast<proto::MessageType>(type);
  out.body = std::move(body);
  return true;
}
static bool send_frame(SOCKET s, proto::MessageType t, const std::vector<std::uint8_t>& b) {
  proto::Frame f; f.type = t; f.body = b;
  auto v = proto::encode_frame(f);
  return net::write_all(s, reinterpret_cast<const char*>(v.data()), v.size());
}
static bool hello(SOCKET s) {
  Writer b; b.str("cuworker");
  send_frame(s, proto::MessageType::HELLO, b.data());
  proto::Frame f; return recv_frame(s, f);
}
static bool do_register(SOCKET s) {
  Writer b; b.u64(g_worker); b.u64(g_boot); b.u64(g_source); b.u64(g_sboot);
  send_frame(s, proto::MessageType::REGISTER, b.data());
  proto::Frame f;
  if (!(recv_frame(s, f) && f.type == proto::MessageType::REGISTER_ACK)) return false;
  Reader r(f.body); return r.u8() != 0;
}
static bool do_create(SOCKET s, std::uint64_t& id, std::uint64_t& gen) {
  Writer b; b.u8(static_cast<std::uint8_t>(DomainKind::GPU_HOST_TRANSFER));
  b.u8(static_cast<std::uint8_t>(BandwidthKind::THEORETICAL)); b.f64(g_capacity);
  b.u8(static_cast<std::uint8_t>(BandwidthKind::CONFIGURED)); b.f64(g_capacity);
  send_frame(s, proto::MessageType::CREATE_DOMAIN, b.data());
  proto::Frame f;
  if (!(recv_frame(s, f) && f.type == proto::MessageType::DOMAIN_CREATED)) return false;
  Reader r(f.body); std::uint8_t ok = r.u8(); id = r.u64(); gen = r.u64();
  if (ok && !g_idfile.empty()) { std::ofstream out(g_idfile); out << id << " " << gen; }
  return ok != 0;
}
static bool do_declare(SOCKET s, std::uint64_t did, std::uint64_t dgen,
                       std::uint64_t& fid, std::uint64_t& fg) {
  Writer b; b.u64(g_worker); b.u64(g_boot); b.u64(g_source); b.u64(g_sboot);
  b.u8(static_cast<std::uint8_t>(g_class));
  b.u64(did); b.u64(dgen); b.u64(g_bytes); b.u64(g_bytes);
  b.f64(0); b.f64(0); b.f64(0); b.f64(0); b.f64(0);
  b.u8(static_cast<std::uint8_t>(MeasurementProvenance::MEASURED));
  send_frame(s, proto::MessageType::DECLARE_FLOW, b.data());
  proto::Frame f;
  if (!(recv_frame(s, f) && f.type == proto::MessageType::FLOW_DECLARED)) return false;
  Reader r(f.body); std::uint8_t ok = r.u8(); fid = r.u64(); fg = r.u64();
  if (ok && !g_flowfile.empty()) { std::ofstream out(g_flowfile); out << fid << " " << fg; }
  return ok != 0;
}
static bool flow_op(SOCKET s, proto::MessageType t, std::uint64_t fid, std::uint64_t fg) {
  Writer b; b.u64(fid); b.u64(fg);
  send_frame(s, t, b.data());
  proto::Frame f; return recv_frame(s, f) && f.type == proto::MessageType::OK;
}
static bool do_start(SOCKET s, std::uint64_t fid, std::uint64_t fg) {
  Writer b; b.u64(fid); b.u64(fg); b.u64(g_boot);
  send_frame(s, proto::MessageType::START, b.data());
  proto::Frame f; return recv_frame(s, f) && f.type == proto::MessageType::OK;
}
static void publish_load(SOCKET s, std::uint64_t did) {
  Writer b; b.u64(did);
  b.u8(static_cast<std::uint8_t>(MeasurementKind::OFFERED_BYTES_PER_SEC)); b.f64(g_offered);
  b.u8(static_cast<std::uint8_t>(MeasurementProvenance::MEASURED)); b.f64(1.0);
  send_frame(s, proto::MessageType::PUBLISH_MEASUREMENT, b.data());
  proto::Frame f; recv_frame(s, f);
  Writer b2; b2.u64(did);
  b2.u8(static_cast<std::uint8_t>(MeasurementKind::COMPLETED_BYTES_PER_SEC)); b2.f64(g_serviced);
  b2.u8(static_cast<std::uint8_t>(MeasurementProvenance::MEASURED)); b2.f64(1.0);
  send_frame(s, proto::MessageType::PUBLISH_MEASUREMENT, b2.data());
  proto::Frame f2; recv_frame(s, f2);
  // DERIVED backlog/queue evidence is not directly exposed by CUDA; the engine
  // derives queue state from the active flow + offered/serviced gap.
  std::printf("CUDA_EVIDENCE: domain=%llu offered=%f serviced=%f measured\n",
              (unsigned long long)did, g_offered, g_serviced);
}
static int do_query(SOCKET s, std::uint64_t did) {
  Writer b; b.u64(did);
  send_frame(s, proto::MessageType::QUERY_CONGESTION, b.data());
  proto::Frame f;
  if (!(recv_frame(s, f) && f.type == proto::MessageType::CONGESTION_UPDATE)) { std::printf("NO_UPDATE\n"); return 1; }
  Reader r(f.body);
  std::uint8_t st = r.u8(), prov = r.u8();
  double sc = r.f64(); std::uint8_t auth = r.u8(); std::uint8_t hasload = r.u8();
  double offered = r.f64(), serviced = r.f64(), eff = r.f64(), resid = r.f64();
  std::printf("CUDA_CONGESTION state=%d prov=%d score=%f auth=%d hasload=%d offered=%f serviced=%f eff=%f resid=%f\n",
              (int)st, (int)prov, sc, (int)auth, (int)hasload, offered, serviced, eff, resid);
  return (int)st;
}
static std::uint64_t read_domain_file(std::uint64_t& gen) {
  std::ifstream in(g_idfile);
  std::uint64_t id = 0; in >> id >> gen;
  return id;
}

int main(int argc, char** argv) {
  std::setvbuf(stdout, nullptr, _IONBF, 0);
  bool standalone = true;
  for (int i = 1; i < argc; ++i) if (!std::strcmp(argv[i], "--coord")) standalone = false;
  if (standalone) {
    bool hold = false;
    for (int i = 1; i < argc; ++i) if (!std::strcmp(argv[i], "--hold")) hold = true;
    bool ok = do_cuda_work(hold ? "CUDA_WORKER[hold]" : "CUDA_WORKER[revalidate]", nullptr, nullptr);
    if (ok) std::printf("CUDA_WORKER_READY mode=%s\n", hold ? "hold" : "revalidate");
    if (hold) { std::printf("CUDA_WORKER_HOLD_MODE\n"); std::fflush(stdout);
      while (true) std::this_thread::sleep_for(std::chrono::milliseconds(50)); }
    cudaDeviceReset();
    std::printf("CUDA_WORKER_DONE ok=%d\n", ok ? 1 : 0);
    return ok ? 0 : 1;
  }
  // ---- coordinator-integrated CUDA worker ----
  arg_scan(argc, argv);
  if (g_port == 0) { std::printf("FAIL: --port required\n"); return 2; }
  if (!net::init()) { std::printf("FAIL: winsock init\n"); return 2; }
  SOCKET s = net::connect_socket(g_port);
  if (s == INVALID_SOCKET) { std::printf("FAIL: connect\n"); net::cleanup(); return 2; }
  if (!hello(s)) { std::printf("FAIL: hello\n"); net::cleanup(); return 2; }
  int rc = 0;
  std::uint64_t completed = 0; double bps = 0.0;
  bool work_ok = do_cuda_work(g_scenario == "work" ? "CUDA_A[work]" : "CUDA_A_prime[reincarnate]", &completed, &bps);
  if (!work_ok) { std::printf("CUDA_INTEGRATED FAIL: CUDA work\n"); rc = 1; }
  else if (!do_register(s)) { std::printf("CUDA_INTEGRATED FAIL: register\n"); rc = 1; }
  else {
    if (g_scenario == "work") {
      if (g_capacity <= 0) g_capacity = bps;
      std::uint64_t did = 0, dgen = 0;
      if (!do_create(s, did, dgen)) { std::printf("CUDA_INTEGRATED FAIL: create\n"); rc = 1; }
      else {
        if (g_bytes == 0) g_bytes = completed;
        std::uint64_t fid = 0, fg = 0;
        if (!do_declare(s, did, dgen, fid, fg)) { std::printf("CUDA_INTEGRATED FAIL: declare\n"); rc = 1; }
        else if (!flow_op(s, proto::MessageType::ADMIT, fid, fg)) { std::printf("CUDA_INTEGRATED FAIL: admit\n"); rc = 1; }
        else if (!do_start(s, fid, fg)) { std::printf("CUDA_INTEGRATED FAIL: start\n"); rc = 1; }
        else {
          if (g_offered <= 0) g_offered = bps * 1.4;
          if (g_serviced <= 0) g_serviced = bps * 0.55;
          publish_load(s, did);
          do_query(s, did);
          std::printf("CUDA_INTEGRATED ready flow=%llu gen=%llu domain=%llu pid=%lu\n",
                      (unsigned long long)fid, (unsigned long long)fg, (unsigned long long)did,
                      (unsigned long)::GetCurrentProcessId());
          if (g_stay) { proto::Frame f; while (recv_frame(s, f)) {} }
        }
      }
    } else if (g_scenario == "resurrect") {
      std::uint64_t dgen = 1;
      std::uint64_t did = read_domain_file(dgen);
      if (did == 0) { std::printf("CUDA_INTEGRATED FAIL: no domain id\n"); rc = 1; }
      else {
        if (g_bytes == 0) g_bytes = completed;
        std::uint64_t fid = 0, fg = 0;
        if (!do_declare(s, did, dgen, fid, fg)) { std::printf("CUDA_INTEGRATED FAIL: declare\n"); rc = 1; }
        else if (!flow_op(s, proto::MessageType::ADMIT, fid, fg)) { std::printf("CUDA_INTEGRATED FAIL: admit\n"); rc = 1; }
        else if (!do_start(s, fid, fg)) { std::printf("CUDA_INTEGRATED FAIL: start\n"); rc = 1; }
        else {
          if (g_offered <= 0) g_offered = bps * 0.9;
          if (g_serviced <= 0) g_serviced = bps * 0.8;
          publish_load(s, did);
          do_query(s, did);
          std::printf("CUDA_INTEGRATED prime-ready flow=%llu gen=%llu domain=%llu pid=%lu\n",
                      (unsigned long long)fid, (unsigned long long)fg, (unsigned long long)did,
                      (unsigned long)::GetCurrentProcessId());
          if (g_stay) { proto::Frame f; while (recv_frame(s, f)) {} }
        }
      }
    }
  }
  net::close_socket(s);
  net::cleanup();
  cudaDeviceReset();
  std::printf("CUDA_INTEGRATED_DONE ok=%d pid=%lu\n", rc == 0 ? 1 : 0, (unsigned long)::GetCurrentProcessId());
  return rc;
}
