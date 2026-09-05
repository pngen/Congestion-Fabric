// Congestion Fabric -- scale benchmark.
// Exercises flow publication, progress/completion, queue update, congestion
// assessment, bottleneck id, residual-capacity query, fairness, backpressure,
// persistence save/load and protocol encode/decode at 1k / 10k / 100k flow
// scales. Reports exact scale, configuration and completed operations-per-second.
#include "congestion_fabric/engine.hpp"
#include "congestion_fabric/protocol.hpp"
#include <chrono>
#include <cstdio>
#include <random>
#include <vector>

using namespace congfabric;

static double now_s() {
  using namespace std::chrono;
  return duration_cast<duration<double>>(steady_clock::now().time_since_epoch()).count();
}

static void run_scale(std::uint64_t N) {
  std::mt19937 rng(N);
  double t0 = now_s();
  CongestionFabric eng;
  CapacityProfile cap;
  cap.set_theoretical(Rate(1e9, BandwidthKind::THEORETICAL, MeasurementProvenance::REPORTED));
  auto dom = eng.create_domain(DomainKind::NIC_TX, cap);
  eng.register_worker(WorkerId(1), WorkerBootId(1));
  eng.register_source(SourceId(1), SourceBootId(1));
  std::vector<EntityRef<Flow>> flows;
  flows.reserve(N);
  for (std::uint64_t i = 0; i < N; ++i) {
    FlowOptions o;
    o.owner_worker = WorkerId(1); o.owner_boot = WorkerBootId(1);
    o.source = SourceId(1); o.source_boot = SourceBootId(1);
    o.domain = dom;
    o.submitted_bytes = 1000 + (rng() % 100000);
    auto fl = eng.declare_flow(o);
    eng.admit_flow(fl);
    flows.push_back(fl);
  }
  double declare_t = now_s() - t0;
  std::printf("SCALE=%llu	declare_admit=%llu ops in %.3f s  (%.0f ops/s)\n",
              (unsigned long long)N, (unsigned long long)N * 2, declare_t,
              (double)(N * 2) / declare_t);

  t0 = now_s();
  for (std::uint64_t i = 0; i < N; ++i) eng.start_flow(flows[i], WorkerBootId(1));
  double start_t = now_s() - t0;
  std::printf("  start=%llu ops in %.3f s (%.0f ops/s)\n", (unsigned long long)N, start_t,
              (double)N / start_t);

  t0 = now_s();
  for (std::uint64_t i = 0; i < N; ++i) eng.flow_progress(flows[i], WorkerBootId(1), 500);
  double prog_t = now_s() - t0;
  std::printf("  progress=%llu ops in %.3f s (%.0f ops/s)\n", (unsigned long long)N, prog_t,
              (double)N / prog_t);

  // congestion assessment + bottleneck + residual + fairness + backpressure
  Measurement mo; mo.kind = MeasurementKind::OFFERED_BYTES_PER_SEC;
  mo.value = 2e9; mo.provenance = MeasurementProvenance::REPORTED;
  eng.publish_measurement(dom.id, mo);
  Measurement ms; ms.kind = MeasurementKind::COMPLETED_BYTES_PER_SEC;
  ms.value = 5e8; ms.provenance = MeasurementProvenance::REPORTED;
  eng.publish_measurement(dom.id, ms);
  t0 = now_s();
  const int Q = 1000;
  for (int i = 0; i < Q; ++i) {
    auto a = eng.assess_congestion(dom.id);
    auto b = eng.identify_bottleneck({dom.id});
    auto r = eng.query_residual_capacity(dom.id, 2e9);
    auto f = eng.evaluate_fairness(dom.id);
    auto bp = eng.eval_backpressure(dom.id);
    (void)a; (void)b; (void)r; (void)f; (void)bp;
  }
  double assess_t = now_s() - t0;
  std::printf("  congestion-assess/query=%d ops in %.3f s (%.0f ops/s)\n", Q, assess_t, (double)Q / assess_t);

  // persistence save/load
  t0 = now_s();
  const char* path = "bench_state.bin";
  bool saved = eng.save(path);
  double save_t = now_s() - t0;
  t0 = now_s();
  CongestionFabric eng2;
  bool loaded = eng2.load(path);
  double load_t = now_s() - t0;
  std::printf("  persist save=%d in %.3f s, load=%d in %.3f s\n", saved ? 1 : 0, save_t, loaded ? 1 : 0, load_t);
  remove(path);

  // protocol encode/decode
  t0 = now_s();
  proto::Frame f; f.type = proto::MessageType::CONGESTION_UPDATE; f.body.assign(256, 0x42);
  std::vector<std::uint8_t> bytes;
  for (int i = 0; i < 100000; ++i) {
    bytes = proto::encode_frame(f);
  }
  double enc_t = now_s() - t0;
  t0 = now_s();
  for (int i = 0; i < 100000; ++i) { auto d = proto::decode_frame(bytes.data(), bytes.size()); (void)d; }
  double dec_t = now_s() - t0;
  std::printf("  protocol encode=100000 ops in %.3f s (%.0f ops/s), decode=%.3f s (%.0f ops/s)\n",
              enc_t, 100000.0 / enc_t, dec_t, 100000.0 / dec_t);
}

int main() {
  run_scale(1000);
  run_scale(10000);
  run_scale(100000);
  std::printf("benchmark done\n");
  return 0;
}
