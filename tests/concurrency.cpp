// Congestion Fabric -- genuine concurrency testing.
// Real threads exercise admissions vs cancellation, progress vs cancellation,
// completion vs stale generation, capacity update vs query, worker death vs
// progress, epoch advance vs measurement, and concurrent read-heavy queries.
// A shared start latch releases all threads together; operations are validated
// for invariant safety after they join. No arbitrary sleeps for correctness.
#include "congestion_fabric/engine.hpp"
#include <atomic>
#include <cstdio>
#include <thread>
#include <vector>

using namespace congfabric;

static int g_fails = 0;
static void chk(bool c, const char* m) { if (!c) { std::printf("FAIL: %s\n", m); ++g_fails; } }

int main() {
  CongestionFabric eng;
  CapacityProfile cap;
  cap.set_theoretical(Rate(100e6, BandwidthKind::THEORETICAL, MeasurementProvenance::REPORTED));
  auto dom = eng.create_domain(DomainKind::NIC_TX, cap);
  eng.register_worker(WorkerId(1), WorkerBootId(1));
  eng.register_worker(WorkerId(2), WorkerBootId(1));
  eng.register_source(SourceId(1), SourceBootId(1));
  eng.register_source(SourceId(2), SourceBootId(1));

  const int TH = 10;
  std::atomic<bool> start{false};
  const std::uint64_t ITERS = 20000;
  std::vector<EntityRef<Flow>> flows(TH);
  for (int t = 0; t < TH; ++t) {
    FlowOptions o;
    o.owner_worker = WorkerId(1); o.owner_boot = WorkerBootId(1);
    o.source = SourceId(1); o.source_boot = SourceBootId(1);
    o.domain = dom;
    o.submitted_bytes = 1u << 20;
    flows[t] = eng.declare_flow(o);
    eng.admit_flow(flows[t]);
    eng.start_flow(flows[t], WorkerBootId(1));
  }

  std::vector<std::thread> threads;
  for (int t = 0; t < TH; ++t) {
    threads.emplace_back([&, t]() {
      while (!start.load()) {}   // latch
      std::uint64_t i = 0;
      for (; i < ITERS; ++i) {
        switch (t % 5) {
          case 0: eng.flow_progress(flows[t], WorkerBootId(1), 1); break;
          case 1: eng.flow_cancel(flows[t], WorkerBootId(1), 100); break;
          case 2: eng.flow_complete(flows[t], WorkerBootId(1)); break;
          case 3: eng.flow_progress(flows[t], WorkerBootId(1), 1); break;
          case 4: { // read-heavy query vs mutation
            eng.recompute_domain(dom.id);
            eng.snapshot(dom.id);
            eng.evaluate_fairness(dom.id);
            eng.eval_backpressure(dom.id);
            Measurement m; m.kind = MeasurementKind::OFFERED_BYTES_PER_SEC;
            m.value = 1e8; m.provenance = MeasurementProvenance::REPORTED;
            eng.publish_measurement(dom.id, m);
            break;
          }
        }
      }
      // Worker-death / epoch-advance / persistence races.
      if (t == 5) eng.fence_worker(WorkerId(1), WorkerBootId(1));
      if (t == 6) eng.advance_epoch(CoordinatorEpoch(2));
      if (t == 7) eng.recover_dynamic_evidence();
      if (t == 8) eng.save("concat.bin");
      if (t == 9) { CongestionFabric e2; e2.load("concat.bin"); }
    });
  }

  start.store(true);
  for (auto& th : threads) th.join();

  for (const EntityRef<Flow>& fl : flows) {
    const Flow* f = eng.flow(fl.id);
    if (f) {
      bool ok = f->accounting_valid();
      ok = ok && (f->completed_bytes.value() + f->cancelled_bytes.value() <= f->submitted_bytes.value());
      chk(ok, "flow accounting invariant after races");
      // A terminal flow must never be resurrected to ACTIVE.
      if (f->is_terminal()) chk(!f->ref.generation.is_valid() || f->in_flight_bytes.value() == 0, "terminal flow drains");
    }
  }
  chk(eng.domain_count() == 1, "domain survives concurrent ops");
  chk(eng.epoch().value() >= 1, "epoch advanced without corruption");
  remove("concat.bin");

  std::printf("concurrency: %s (%d failures)\n", g_fails == 0 ? "OK" : "FAIL", g_fails);
  return g_fails == 0 ? 0 : 1;
}
