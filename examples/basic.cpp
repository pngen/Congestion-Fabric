// Congestion Fabric -- comprehensive runnable example.
// Demonstrates basic congestion detection, healthy high-utilization vs real
// congestion, queue-growth, competing flows, local fairness, backpressure,
// cancellation, capacity shrink, worker-death stale-evidence and
// persistence/recovery, and synthetic collective congestion.
#include "congestion_fabric/engine.hpp"
#include <cstdio>

using namespace congfabric;

static CongestionFabric make_engine(CongestionDomainId& dom, double capacity = 100e6) {
  CongestionFabric eng;
  CapacityProfile cap;
  cap.set_theoretical(Rate(capacity, BandwidthKind::THEORETICAL, MeasurementProvenance::MEASURED));
  cap.set_configured(Rate(capacity, BandwidthKind::CONFIGURED, MeasurementProvenance::REPORTED));
  dom = eng.create_domain(DomainKind::NETWORK_PATH, cap).id;
  eng.register_worker(WorkerId(1), WorkerBootId(1));
  eng.register_source(SourceId(1), SourceBootId(1));
  return eng;
}
static EntityRef<Flow> addFlow(CongestionFabric& eng, CongestionDomainId dom, WorkerId w,
                               WorkerBootId boot, TrafficClass tc, std::uint64_t bytes) {
  FlowOptions o;
  o.owner_worker = w; o.owner_boot = boot;
  o.source = SourceId(w.value()); o.source_boot = SourceBootId(boot.value());
  o.domain.id = dom; o.domain.generation = Generation<CongestionDomainTag>(1);
  o.traffic_class = tc; o.submitted_bytes = bytes; o.expected_bytes = bytes;
  auto fl = eng.declare_flow(o);
  eng.admit_flow(fl);
  eng.start_flow(fl, boot);
  return fl;
}
static void publish(CongestionFabric& eng, CongestionDomainId dom, double offered, double serviced) {
  Measurement mo; mo.kind = MeasurementKind::OFFERED_BYTES_PER_SEC; mo.value = offered;
  mo.provenance = MeasurementProvenance::REPORTED;
  eng.publish_measurement(dom, mo);
  Measurement ms; ms.kind = MeasurementKind::COMPLETED_BYTES_PER_SEC; ms.value = serviced;
  ms.provenance = MeasurementProvenance::REPORTED;
  eng.publish_measurement(dom, ms);
}

int main() {
  // --- healthy high utilization vs real congestion ---
  {
    CongestionDomainId dom;
    CongestionFabric eng = make_engine(dom, 100e6);
    auto f = addFlow(eng, dom, WorkerId(1), WorkerBootId(1), TrafficClass::THROUGHPUT, 10u << 20);
    publish(eng, dom, 90e6, 90e6);  // high but satisfied
    auto a = eng.recompute_domain(dom);
    std::printf("HEALTHY_HIGH_UTIL: offered=90%% serviced=90%% -> state=%d (healthy if no queue)\n", (int)a.state);
    publish(eng, dom, 140e6, 50e6); // offered exceeds capacity, backlog grows
    auto a2 = eng.recompute_domain(dom);
    std::printf("REAL_CONGESTION: offered=140%% serviced=50%% -> state=%d\n", (int)a2.state);
    (void)f;
  }
  // --- competing flows, fairness, backpressure ---
  {
    CongestionDomainId dom;
    CongestionFabric eng = make_engine(dom, 100e6);
    eng.register_worker(WorkerId(2), WorkerBootId(1));
    eng.register_source(SourceId(2), SourceBootId(1));
    addFlow(eng, dom, WorkerId(1), WorkerBootId(1), TrafficClass::BULK_TRANSFER, 20u << 20);
    addFlow(eng, dom, WorkerId(2), WorkerBootId(1), TrafficClass::LATENCY_CRITICAL, 20u << 20);
    publish(eng, dom, 160e6, 60e6);
    auto a = eng.recompute_domain(dom);
    auto fair = eng.evaluate_fairness(dom);
    auto bp = eng.eval_backpressure(dom);
    auto snap = eng.snapshot(dom);
    std::printf("COMPETING_FLOWS: state=%d fair_share=%zu backpressure=%d dominant=%llu\n",
                (int)a.state, fair.shares.size(), (int)bp.intent,
                (unsigned long long)snap.dominant_contributor.value());
  }
  // --- cancellation: closes accounting exactly ---
  {
    CongestionDomainId dom;
    CongestionFabric eng = make_engine(dom, 100e6);
    auto fl = addFlow(eng, dom, WorkerId(1), WorkerBootId(1), TrafficClass::CHECKPOINT, 1000);
    eng.flow_progress(fl, WorkerBootId(1), 400);
    eng.flow_cancel(fl, WorkerBootId(1), 600);
    const Flow* f = eng.flow(fl.id);
    if (f) std::printf("CANCEL: completed=%llu cancelled=%llu admitted=%llu state=%d\n",
                       (unsigned long long)f->completed_bytes.value(),
                       (unsigned long long)f->cancelled_bytes.value(),
                       (unsigned long long)f->admitted_bytes.value(), (int)f->state);
  }
  // --- capacity shrink below load ---
  {
    CongestionDomainId dom;
    CongestionFabric eng = make_engine(dom, 100e6);
    auto fl = addFlow(eng, dom, WorkerId(1), WorkerBootId(1), TrafficClass::BULK_TRANSFER, 80u << 20);
    publish(eng, dom, 120e6, 60e6);
    auto a = eng.recompute_domain(dom);
    // Attempt a reservation exceeding capacity must be rejected on create.
    CapacityProfile cap2;
    cap2.set_theoretical(Rate(50e6, BandwidthKind::THEORETICAL, MeasurementProvenance::REPORTED));
    cap2.set_reserved(Rate(60e6, BandwidthKind::RESERVED, MeasurementProvenance::REPORTED));
    auto invalidDom = eng.create_domain(DomainKind::NETWORK_PATH, cap2);
    std::printf("CAPACITY_SHRINK: state_on_initial=%d, reservation-exceeding-capacity valid=%d\n",
                (int)a.state, invalidDom.id.is_valid() ? 1 : 0);
    (void)fl;
  }
  // --- worker death stale evidence ---
  {
    CongestionDomainId dom;
    CongestionFabric eng = make_engine(dom, 100e6);
    auto fl = addFlow(eng, dom, WorkerId(1), WorkerBootId(1), TrafficClass::RECOVERY, 1000);
    eng.flow_progress(fl, WorkerBootId(1), 300);
    bool fenced = eng.fence_worker(WorkerId(1), WorkerBootId(1));
    const Flow* f = eng.flow(fl.id);
    bool staleRejected = !eng.flow_progress(fl, WorkerBootId(1), 100);
    std::printf("WORKER_DEATH: fenced=%d flow_state=%d stale_progress_rejected=%d\n",
                fenced ? 1 : 0, f ? (int)f->state : -1, staleRejected ? 1 : 0);
  }
  // --- persistence / recovery ---
  {
    CongestionDomainId dom;
    CongestionFabric eng = make_engine(dom, 100e6);
    addFlow(eng, dom, WorkerId(1), WorkerBootId(1), TrafficClass::MODEL_LOAD, 2000);
    if (eng.save("example_state.bin")) {
      CongestionFabric eng2;
      bool loaded = eng2.load("example_state.bin");
      eng2.recover_dynamic_evidence();
      std::printf("PERSISTENCE: saved+loaded=%d; dynamic evidence revalidated after recovery\n", loaded ? 1 : 0);
    }
    remove("example_state.bin");
  }
  // --- synthetic collective congestion ---
  {
    CongestionFabric eng;
    CapacityProfile cap;
    cap.set_theoretical(Rate(25e6, BandwidthKind::THEORETICAL, MeasurementProvenance::SYNTHETIC));
    auto dom = eng.create_domain(DomainKind::COLLECTIVE_PATH, cap);
    eng.register_worker(WorkerId(1), WorkerBootId(1));
    eng.register_source(SourceId(1), SourceBootId(1));
    addFlow(eng, dom.id, WorkerId(1), WorkerBootId(1), TrafficClass::COLLECTIVE, 30u << 20);
    publish(eng, dom.id, 20e6, 12e6);
    auto a = eng.recompute_domain(dom.id);
    std::printf("SYNTHETIC_COLLECTIVE: state=%d (synthetic; no physical multi-GPU)\n", (int)a.state);
  }
  std::printf("example done\n");
  return 0;
}
