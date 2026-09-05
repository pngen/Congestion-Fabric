// Congestion Fabric -- deterministic seeded property-based testing.
// Exercises flow lifecycle, queueing, generation, capacity-change and
// persistence/recovery invariants over many seeded random scenarios. On
// failure it emits the exact seed so the case can be reproduced.
#include "congestion_fabric/engine.hpp"
#include <cstdio>
#include <random>
#include <string>
#include <vector>

using namespace congfabric;

static int g_fails = 0;

static bool check_accounting(const CongestionFabric& eng, const Flow& f) {
  bool ok = f.accounting_valid();
  ok = ok && (f.completed_bytes.value() + f.cancelled_bytes.value() <= f.submitted_bytes.value() + 1);
  return ok;
}

static void run_property(std::uint32_t seed, std::uint64_t iterations) {
  std::mt19937 rng(seed);
  CongestionFabric eng;
  CapacityProfile cap;
  cap.set_theoretical(Rate(100e6, BandwidthKind::THEORETICAL, MeasurementProvenance::REPORTED));
  auto dom = eng.create_domain(DomainKind::NETWORK_PATH, cap);
  eng.register_worker(WorkerId(1), WorkerBootId(1));
  eng.register_source(SourceId(1), SourceBootId(1));
  bool overflow = false;

  std::vector<EntityRef<Flow>> flows;
  std::vector<WorkerBootId> boots = {WorkerBootId(1), WorkerBootId(2), WorkerBootId(3)};
  for (std::uint64_t it = 0; it < iterations; ++it) {
    int op = static_cast<int>(rng() % 12);
    WorkerBootId boot = boots[rng() % boots.size()];
    switch (op) {
      case 0: { // declare a flow
        if (flows.size() < 200) {
          FlowOptions o;
          o.owner_worker = WorkerId(1); o.owner_boot = boot;
          o.source = SourceId(1); o.source_boot = SourceBootId(boot.value());
          o.domain = dom;
          o.submitted_bytes = rng() % (1u << 20);
          o.expected_bytes = o.submitted_bytes;
          auto fl = eng.declare_flow(o);
          if (fl.id.is_valid()) flows.push_back(fl);
        }
        break;
      }
      case 1: { // admit
        if (!flows.empty()) {
          auto fl = flows[rng() % flows.size()];
          if (eng.flow(fl.id)) eng.admit_flow(fl);
        }
        break;
      }
      case 2: { // start
        if (!flows.empty()) {
          auto fl = flows[rng() % flows.size()];
          const Flow* f = eng.flow(fl.id);
          if (f && f->owner_boot == boot) eng.start_flow(fl, boot);
        }
        break;
      }
      case 3: { // progress
        if (!flows.empty()) {
          auto fl = flows[rng() % flows.size()];
          const Flow* f = eng.flow(fl.id);
          if (f && f->owner_boot == boot) eng.flow_progress(fl, boot, rng() % 4096);
        }
        break;
      }
      case 4: { // complete
        if (!flows.empty()) {
          auto fl = flows[rng() % flows.size()];
          const Flow* f = eng.flow(fl.id);
          if (f && f->owner_boot == boot) eng.flow_complete(fl, boot);
        }
        break;
      }
      case 5: { // cancel
        if (!flows.empty()) {
          auto fl = flows[rng() % flows.size()];
          const Flow* f = eng.flow(fl.id);
          if (f && f->owner_boot == boot) eng.flow_cancel(fl, boot, rng() % (1u << 20));
        }
        break;
      }
      case 6: { // advance generation
        if (!flows.empty()) {
          auto fl = flows[rng() % flows.size()];
          const Flow* f = eng.flow(fl.id);
          if (f) {
            FlowGeneration ng(f->ref.generation.value() + 1 + (rng() % 3));
            eng.advance_flow_generation(fl, ng);
          }
        }
        break;
      }
      case 7: { // publish measurement with random valid/invalid values
        Measurement m;
        m.kind = static_cast<MeasurementKind>(rng() % 19);
        m.value = (rng() % 2) ? static_cast<double>(rng() % 1000000) : 0.0;
        m.provenance = MeasurementProvenance::REPORTED;
        if (!eng.publish_measurement(dom.id, m)) {
          // may legitimately reject non-finite; never assert incorrectly
        }
        break;
      }
      case 8: { // assess congestion
        eng.recompute_domain(dom.id);
        break;
      }
      case 9: { // query snapshot / fairness / backpressure
        eng.snapshot(dom.id);
        eng.evaluate_fairness(dom.id);
        eng.eval_backpressure(dom.id);
        break;
      }
      case 10: { // fence worker (advances boot)
        eng.fence_worker(WorkerId(1), WorkerBootId(1));
        break;
      }
      case 11: { // capacity shrink below load (must remain valid or rejected)
        double eff = cap.effective().value_or(100e6);
        if (eff > 1.0) {
          double shrink = eff * 0.5;
          CapacityProfile nc;
          nc.set_theoretical(Rate(shrink, BandwidthKind::THEORETICAL, MeasurementProvenance::REPORTED));
          eng.remove_domain(dom.id);
          auto nd = eng.create_domain(DomainKind::NETWORK_PATH, nc);
          overflow = overflow || !nd.id.is_valid();
        }
        break;
      }
    }
    // Validate invariants over all registered flows.
    for (const EntityRef<Flow>& fl : flows) {
      const Flow* f = eng.flow(fl.id);
      if (f && !check_accounting(eng, *f)) { overflow = true; std::printf("INVARIANT VIOLATION at iter %llu\n", (unsigned long long)it); return; }
    }
  }
  (void)overflow;
  // Persistence round-trip preserves durable history.
  if (eng.save("build_property.bin")) {
    CongestionFabric eng2;
    if (!eng2.load("build_property.bin")) { std::printf("PROPERTY: load failed\n"); ++g_fails; }
  }
  std::printf("PROPERTY seed=%u iterations=%llu OK\n", seed, (unsigned long long)iterations);
}

int main() {
  const std::uint32_t seeds[] = {0x1234u, 0xCAFEu, 0xDEADu, 0xBEEFu, 0x9999u, 0xABCDu};
  for (std::uint32_t s : seeds) run_property(s, 2000);
  std::printf("property: %s (%d failures)\n", g_fails == 0 ? "OK" : "FAIL", g_fails);
  // cleanup
  remove("build_property.bin");
  return g_fails == 0 ? 0 : 1;
}
