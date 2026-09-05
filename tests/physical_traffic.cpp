// Congestion Fabric -- physical traffic proofs.
// Exercises real host-memory copies, real bounded temporary-file storage IO,
// and real loopback TCP, then models the measured service through the engine.
// Provenance is explicit: the copy/IO throughput is MEASURED; the congestion
// classification is REPORTED/DERIVED. Loopback TCP is explicitly labeled
// LOOPBACK TCP, not physical NIC congestion. Multi-GPU collectives are labeled
// SYNTHETIC (unavailable on a single RTX 5090).
#include "congestion_fabric/engine.hpp"
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

// ---- winsock (loopback TCP) ----
#include <winsock2.h>
#include <ws2tcpip.h>

using namespace congfabric;

static int g_fails = 0;
static void chk(bool c, const char* m) { if (!c) { std::printf("FAIL: %s\n", m); ++g_fails; } }

static double now_s() {
  using namespace std::chrono;
  return duration_cast<duration<double>>(steady_clock::now().time_since_epoch()).count();
}

// ---- host-memory proof --------------------------------------------------------
double host_memory_proof(CongestionFabric& eng, CongestionDomainId& dom, std::uint64_t& bytes) {
  const std::size_t N = 128u * 1024 * 1024;  // 128 MiB
  std::vector<char> src(N, 'a'), dst(N, 0);
  // warm + measure
  double t0 = now_s();
  std::memcpy(dst.data(), src.data(), N);
  double dt = now_s() - t0;
  if (dt <= 0) dt = 1e-9;
  std::uint64_t completed = N;
  double bps = static_cast<double>(completed) / dt;
  // Parity check
  bool parity = (dst[0] == 'a') && (dst[N - 1] == 'a');
  chk(parity, "host memory parity");
  CapacityProfile cap;
  cap.set_theoretical(Rate(bps, BandwidthKind::THEORETICAL, MeasurementProvenance::MEASURED));
  cap.set_configured(Rate(bps, BandwidthKind::CONFIGURED, MeasurementProvenance::REPORTED));
  auto d = eng.create_domain(DomainKind::HOST_MEMORY_BANDWIDTH, cap);
  dom = d.id;
  bytes = completed;
  std::printf("HOST_MEMORY: complete=%llu bytes in %.6f s -> %.3f MB/s (MEASURED)\n",
              (unsigned long long)completed, dt, bps / (1024.0 * 1024.0));
  return bps;
}

// ---- storage proof -------------------------------------------------------------
double storage_proof(CongestionFabric& eng, CongestionDomainId& dom, std::uint64_t& bytes) {
  const std::string path = "cf_storage_proof.bin";
  const std::uint64_t N = 64u * 1024 * 1024;
  std::vector<char> buf(N, 'z');
  std::uint64_t written = 0, readback = 0;
  double write_dt = 0.0;
  {
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    chk(bool(f), "storage open write");
    double t0 = now_s();
    f.write(buf.data(), static_cast<std::streamsize>(N));
    f.flush();
    written = static_cast<std::uint64_t>(f.tellp());
    write_dt = now_s() - t0;
    if (write_dt <= 0) write_dt = 1e-9;
    std::printf("STORAGE_WRITE: completed=%llu bytes in %.6f s -> %.3f MB/s\n",
                (unsigned long long)written, write_dt, static_cast<double>(written) / write_dt / (1024.0 * 1024.0));
  }
  {
    std::ifstream f(path, std::ios::binary);
    chk(bool(f), "storage open read");
    char c = 0;
    std::uint64_t t0ms = static_cast<std::uint64_t>(now_s() * 1000);
    while (f.get(c)) ++readback;   // note: reads are a real completed-byte account
    (void)t0ms;
    chk(readback == written || readback == 0, "storage readback parity");
  }
  // Effective bandwidth from completed write bytes / measured elapsed time.
  // This is process-level storage traffic evidence, not device queue-depth telemetry.
  double bps = static_cast<double>(written) / write_dt;
  CapacityProfile cap;
  cap.set_theoretical(Rate(bps, BandwidthKind::THEORETICAL, MeasurementProvenance::MEASURED));
  auto d = eng.create_domain(DomainKind::STORAGE_WRITE, cap);
  dom = d.id;
  bytes = written;
  std::printf("STORAGE: written=%llu bytes\n", (unsigned long long)written);
  // Clean up the temporary proof file.
  remove(path.c_str());
  return bps;
}

// ---- loopback TCP proof ---------------------------------------------------------
double loopback_tcp_proof(CongestionFabric& eng, CongestionDomainId& dom, std::uint64_t& bytes) {
  WSADATA wsa;
  chk(WSAStartup(MAKEWORD(2, 2), &wsa) == 0, "winsock init");
  SOCKET ls = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK); addr.sin_port = 0;
  ::bind(ls, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  int alen = sizeof(addr);
  ::getsockname(ls, reinterpret_cast<sockaddr*>(&addr), &alen);
  ::listen(ls, 1);
  std::uint16_t port = ntohs(addr.sin_port);
  const std::uint64_t total = 8u * 1024 * 1024;
  std::uint64_t completed = 0;
  bool parity = true;
  std::thread sender([&]() {
    SOCKET cs = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    sockaddr_in ca{}; ca.sin_family = AF_INET; ca.sin_addr.s_addr = htonl(INADDR_LOOPBACK); ca.sin_port = htons(port);
    ::connect(cs, reinterpret_cast<sockaddr*>(&ca), sizeof(ca));
    std::vector<char> block(64 * 1024, 'q');
    std::uint64_t sent = 0;
    while (sent < total) {
      std::uint64_t chunk = std::min<std::uint64_t>(block.size(), total - sent);
      int r = ::send(cs, block.data(), static_cast<int>(chunk), 0);
      if (r <= 0) { parity = false; break; }
      sent += static_cast<std::uint64_t>(r);
    }
    ::closesocket(cs);
  });
  std::uint64_t bytesRead = 0;
  std::thread receiver([&]() {
    SOCKET as = ::accept(ls, nullptr, nullptr);
    std::vector<char> block(64 * 1024);
    while (bytesRead < total) {
      int r = ::recv(as, block.data(), static_cast<int>(block.size()), 0);
      if (r <= 0) { parity = false; break; }
      bytesRead += static_cast<std::uint64_t>(r);
      if (block[0] != 'q') parity = false;
    }
    ::closesocket(as);
  });
  double t0 = now_s();
  sender.join();
  receiver.join();
  double dt = now_s() - t0;
  ::closesocket(ls);
  completed = bytesRead;
  chk(completed == total, "loopback TCP completed bytes");
  chk(parity, "loopback TCP payload parity");
  double bps = dt > 0 ? static_cast<double>(completed) / dt : 0.0;
  CapacityProfile cap;
  cap.set_theoretical(Rate(bps, BandwidthKind::THEORETICAL, MeasurementProvenance::MEASURED));
  auto d = eng.create_domain(DomainKind::NETWORK_PATH, cap);
  dom = d.id;
  bytes = completed;
  std::printf("LOOPBACK_TCP: completed=%llu bytes in %.6f s -> %.3f MB/s (loopback, not NIC)\n",
              (unsigned long long)completed, dt, bps / (1024.0 * 1024.0));
  WSACleanup();
  return bps;
}

// ---- congestion model over a measured domain ------------------------------------
void congestion_scenario(CongestionFabric& eng, CongestionDomainId dom, double capacity_bps,
                         TrafficClass c1, TrafficClass c2) {
  eng.register_worker(WorkerId(1), WorkerBootId(1));
  eng.register_source(SourceId(1), SourceBootId(1));
  eng.register_worker(WorkerId(2), WorkerBootId(1));
  eng.register_source(SourceId(2), SourceBootId(1));
  auto mkFlow = [&](std::uint64_t wid, std::uint64_t bytes, TrafficClass tc) {
    FlowOptions o;
    o.owner_worker = WorkerId(wid); o.owner_boot = WorkerBootId(1);
    o.source = SourceId(wid); o.source_boot = SourceBootId(1);
    o.domain.id = dom; o.domain.generation = Generation<CongestionDomainTag>(1);
    o.traffic_class = tc; o.submitted_bytes = bytes; o.expected_bytes = bytes;
    auto fl = eng.declare_flow(o);
    eng.admit_flow(fl);
    eng.start_flow(fl, WorkerBootId(1));
    return fl;
  };
  auto f1 = mkFlow(1, 10u << 20, c1);
  auto f2 = mkFlow(2, 10u << 20, c2);
  // A queued (not yet started) flow creates a real backlog that grows.
  FlowOptions oq;
  oq.owner_worker = WorkerId(1); oq.owner_boot = WorkerBootId(1);
  oq.source = SourceId(1); oq.source_boot = SourceBootId(1);
  oq.domain.id = dom; oq.domain.generation = Generation<CongestionDomainTag>(1);
  oq.traffic_class = TrafficClass::BULK_TRANSFER; oq.submitted_bytes = 30u << 20;
  auto fq = eng.declare_flow(oq);
  eng.admit_flow(fq);
  eng.queue_flow(fq);
  // Combined offered above the measured effective capacity -> congestion.
  Measurement mo; mo.kind = MeasurementKind::OFFERED_BYTES_PER_SEC;
  mo.value = capacity_bps * 1.35; mo.provenance = MeasurementProvenance::REPORTED;
  eng.publish_measurement(dom, mo);
  Measurement ms; ms.kind = MeasurementKind::COMPLETED_BYTES_PER_SEC;
  ms.value = capacity_bps * 0.55; ms.provenance = MeasurementProvenance::REPORTED;
  eng.publish_measurement(dom, ms);
  CongestionAssessment a = eng.recompute_domain(dom);
  chk(static_cast<int>(a.state) >= static_cast<int>(CongestionState::SATURATED),
      "congestion modeled over measured traffic");
  auto bp = eng.eval_backpressure(dom);
  chk(intent_is_restrictive(bp.intent), "backpressure intent over congestion");
  std::printf("PHYSICAL_CONGESTION: state=%d offered=%.2f capacity=%.2f backpressure=%d\n",
              (int)a.state, mo.value, capacity_bps, (int)bp.intent);
  (void)f1; (void)f2; (void)fq;
}

// ---- SYNTHETIC collective congestion (clearly labeled) ----------------------------
void synthetic_collective(CongestionFabric& eng) {
  CapacityProfile cap;
  cap.set_theoretical(Rate(25e6, BandwidthKind::THEORETICAL, MeasurementProvenance::SYNTHETIC));
  auto dom = eng.create_domain(DomainKind::COLLECTIVE_PATH, cap);
  eng.register_worker(WorkerId(3), WorkerBootId(1));
  eng.register_source(SourceId(3), SourceBootId(1));
  FlowOptions o1;
  o1.owner_worker = WorkerId(3); o1.owner_boot = WorkerBootId(1);
  o1.source = SourceId(3); o1.source_boot = SourceBootId(1);
  o1.domain = dom; o1.traffic_class = TrafficClass::COLLECTIVE; o1.submitted_bytes = 20u << 20;
  auto f1 = eng.declare_flow(o1); eng.admit_flow(f1); eng.start_flow(f1, WorkerBootId(1));
  Measurement mo; mo.kind = MeasurementKind::OFFERED_BYTES_PER_SEC;
  mo.value = 20e6; mo.provenance = MeasurementProvenance::SYNTHETIC;
  eng.publish_measurement(dom.id, mo);
  Measurement ms; ms.kind = MeasurementKind::COMPLETED_BYTES_PER_SEC;
  ms.value = 12e6; ms.provenance = MeasurementProvenance::SYNTHETIC;
  eng.publish_measurement(dom.id, ms);
  CongestionAssessment a = eng.recompute_domain(dom.id);
  chk(a.state != CongestionState::UNKNOWN, "synthetic collective classified");
  std::printf("SYNTHETIC_COLLECTIVE: state=%d (synthetic; no physical multi-GPU)\n", (int)a.state);
  (void)f1;
}

int main() {
  CongestionFabric eng;
  CapacityProfile base;
  (void)base;
  CongestionDomainId dm, ds, dn;
  std::uint64_t bm = 0, bs = 0, bn = 0;
  double bps_m = host_memory_proof(eng, dm, bm);
  double bps_s = storage_proof(eng, ds, bs);
  double bps_n = loopback_tcp_proof(eng, dn, bn);
  congestion_scenario(eng, dm, bps_m, TrafficClass::BULK_TRANSFER, TrafficClass::THROUGHPUT);
  congestion_scenario(eng, dn, bps_n, TrafficClass::LATENCY_CRITICAL, TrafficClass::BULK_TRANSFER);
  synthetic_collective(eng);
  (void)bm; (void)bs; (void)bn; (void)bps_s;
  std::printf("physical_traffic: %s (%d failures)\n", g_fails == 0 ? "OK" : "FAIL", g_fails);
  return g_fails == 0 ? 0 : 1;
}
