// Congestion Fabric -- CLI inspection tool.
// Dumps congestion domains, generations, flows, queue/backlog, offered/serviced
// load, effective/residual bandwidth, congestion state, congestion episodes,
// dominant contributor, fairness deficit, starvation, backpressure intent,
// provenance, and stale/revalidation state. Loads a saved state file if given,
// otherwise builds a representative demo.
#include "congestion_fabric/engine.hpp"
#include <cstdio>
#include <cstring>
#include <string>

using namespace congfabric;

static const char* state_name(CongestionState s) {
  static const char* n[] = {"UNKNOWN","IDLE","HEALTHY","BUSY","APPROACHING_SATURATION","SATURATED",
                            "QUEUEING","CONGESTED","SEVERELY_CONGESTED","BACKPRESSURED","RECOVERING",
                            "STALE","REVALIDATION_REQUIRED"};
  return (static_cast<int>(s) >= 0 && static_cast<int>(s) < 13) ? n[static_cast<int>(s)] : "?";
}
static const char* prov_name(MeasurementProvenance p) {
  static const char* n[] = {"MEASURED","REPORTED","DERIVED","ESTIMATED","SYNTHETIC","RECONSTRUCTED","UNKNOWN"};
  return (static_cast<int>(p) >= 0 && static_cast<int>(p) < 7) ? n[static_cast<int>(p)] : "?";
}

static void dump(const CongestionFabric& eng, bool demo) {
  std::printf("=== Congestion Fabric inspection ===\n");
  if (demo) std::printf("mode: demo\n"); else std::printf("mode: loaded state\n");
  std::printf("epoch=%llu domains=%zu live_flows=%llu\n",
              (unsigned long long)eng.epoch().value(), eng.domain_count(),
              (unsigned long long)eng.live_flow_count());
  for (CongestionDomainId did : eng.domain_ids()) {
    DomainSnapshot s = eng.snapshot(did);
    std::printf("\n[domain %llu gen %llu kind=%d] state=%s (%s)\n",
                (unsigned long long)s.ref.id.value(), (unsigned long long)s.ref.generation.value(),
                (int)s.kind, state_name(s.state), prov_name(s.provenance));
    std::printf("  offered=%.2f serviced=%.2f effective=%.2f residual=%.2f utilization=%.3f\n",
                s.offered_bps, s.serviced_bps, s.effective_bps, s.residual_bps, s.utilization);
    std::printf("  queued_bytes=%llu queued_ops=%llu in_flight_bytes=%llu\n",
                (unsigned long long)s.queued_bytes, (unsigned long long)s.queued_ops,
                (unsigned long long)s.in_flight_bytes);
    std::printf("  dominant_contributor=%llu dominant_class=%d upstream_bottleneck=%llu\n",
                (unsigned long long)s.dominant_contributor.value(), (int)s.dominant_class,
                (unsigned long long)s.upstream_bottleneck.value());
    std::printf("  %s\n", s.summary.c_str());
    FairnessReport fr = eng.evaluate_fairness(did);
    for (const FairnessShare& sh : fr.shares) {
      std::printf("  fairness flow=%llu target=%.2f observed=%.2f deficit=%.2f starved=%d\n",
                  (unsigned long long)sh.flow.value(), sh.target_share_bps, sh.observed_share_bps,
                  sh.fairness_deficit_bps, sh.starved ? 1 : 0);
    }
    BackpressureAction bp = eng.eval_backpressure(did);
    std::printf("  backpressure intent=%d cause=%s\n", (int)bp.intent, bp.cause.c_str());
    auto hist = eng.episode_history(did);
    for (const CongestionEpisode& e : hist) {
      std::printf("  episode id=%llu gen=%llu peak=%s start=%llu end=%llu open=%d\n",
                  (unsigned long long)e.event_id.value(), (unsigned long long)e.generation.value(),
                  state_name(e.peak_state), (unsigned long long)e.start_ms, (unsigned long long)e.end_ms,
                  e.open ? 1 : 0);
    }
  }
  // flows (via a hidden enumeration is not exposed; report live flow count only)
  std::printf("\n(inspection of individual flows uses the engine API; count reported above)\n");
}

int main(int argc, char** argv) {
  std::string state;
  for (int i = 1; i < argc; ++i) {
    if (!std::strcmp(argv[i], "--state") && i + 1 < argc) state = argv[++i];
  }
  if (state.empty()) {
    CongestionFabric eng;
    CapacityProfile cap;
    cap.set_theoretical(Rate(100e6, BandwidthKind::THEORETICAL, MeasurementProvenance::MEASURED));
    cap.set_configured(Rate(100e6, BandwidthKind::CONFIGURED, MeasurementProvenance::REPORTED));
    auto dom = eng.create_domain(DomainKind::GPU_HOST_TRANSFER, cap);
    eng.register_worker(WorkerId(1), WorkerBootId(1));
    eng.register_source(SourceId(1), SourceBootId(1));
    FlowOptions o;
    o.owner_worker = WorkerId(1); o.owner_boot = WorkerBootId(1);
    o.source = SourceId(1); o.source_boot = SourceBootId(1);
    o.domain = dom; o.traffic_class = TrafficClass::BULK_TRANSFER; o.submitted_bytes = 50u << 20;
    auto fl = eng.declare_flow(o); eng.admit_flow(fl); eng.start_flow(fl, WorkerBootId(1));
    Measurement mo; mo.kind = MeasurementKind::OFFERED_BYTES_PER_SEC; mo.value = 130e6;
    mo.provenance = MeasurementProvenance::REPORTED;
    eng.publish_measurement(dom.id, mo);
    Measurement ms; ms.kind = MeasurementKind::COMPLETED_BYTES_PER_SEC; ms.value = 60e6;
    ms.provenance = MeasurementProvenance::REPORTED;
    eng.publish_measurement(dom.id, ms);
    eng.recompute_domain(dom.id);
    dump(eng, true);
  } else {
    CongestionFabric eng;
    if (!eng.load(state)) { std::fprintf(stderr, "could not load state: %s\n", state.c_str()); return 2; }
    dump(eng, false);
  }
  return 0;
}
