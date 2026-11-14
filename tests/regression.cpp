// Congestion Fabric regression tests: fencing one worker leaves the domains whose
// evidence another worker owns untouched; after a fence only republished offered
// or serviced load clears a domain's revalidation requirement (an unrelated
// measurement or a flow start does not); a restart does not restore pre-restart
// offered/serviced load as current evidence.
#include "congestion_fabric/engine.hpp"
#include <cstdio>
#include <vector>
using namespace congfabric;

static int fails = 0;
static void chk(bool c, const char* m) {
  if (!c) { std::printf("FAIL: %s\n", m); ++fails; }
}
static Measurement meas(MeasurementKind k, double v) {
  Measurement m; m.kind = k; m.value = v; m.provenance = MeasurementProvenance::REPORTED; m.confidence = 1.0;
  return m;
}
static CapacityProfile cap100() {
  CapacityProfile cap;
  cap.set_theoretical(Rate(100e6, BandwidthKind::THEORETICAL, MeasurementProvenance::MEASURED));
  cap.set_configured(Rate(100e6, BandwidthKind::CONFIGURED, MeasurementProvenance::REPORTED));
  return cap;
}

int main() {
  // Fencing worker 1 does not touch the domains whose evidence worker 2 owns.
  {
    CongestionFabric eng;
    chk(eng.register_worker(WorkerId(1), WorkerBootId(1)), "register worker 1");
    chk(eng.register_worker(WorkerId(2), WorkerBootId(1)), "register worker 2");
    std::vector<CongestionDomainId> fenced_owner, live_owner;
    for (int i = 0; i < 12; ++i) {
      const CongestionDomainId d = eng.create_domain(DomainKind::NETWORK_PATH, cap100()).id;
      const WorkerId owner = (i == 3) ? WorkerId(1) : WorkerId(2);
      chk(eng.publish_measurement(d, meas(MeasurementKind::OFFERED_BYTES_PER_SEC, 50e6), owner, WorkerBootId(1)),
          "publish offered");
      chk(eng.publish_measurement(d, meas(MeasurementKind::COMPLETED_BYTES_PER_SEC, 50e6), owner, WorkerBootId(1)),
          "publish serviced");
      chk(eng.recompute_domain(d).state != CongestionState::UNKNOWN, "domain classified");
      (owner == WorkerId(1) ? fenced_owner : live_owner).push_back(d);
    }
    chk(eng.fence_worker(WorkerId(1), WorkerBootId(1)), "fence worker 1");
    chk(eng.snapshot(fenced_owner[0]).state == CongestionState::REVALIDATION_REQUIRED,
        "the fenced worker's domain needs revalidation");
    for (const CongestionDomainId d : live_owner)
      chk(eng.snapshot(d).state != CongestionState::REVALIDATION_REQUIRED, "another worker's domain untouched");
  }

  // After a fence, only republished offered or serviced load clears revalidation.
  {
    CongestionFabric eng;
    chk(eng.register_worker(WorkerId(1), WorkerBootId(1)), "register worker 1");
    chk(eng.register_worker(WorkerId(2), WorkerBootId(1)), "register worker 2");
    chk(eng.register_source(SourceId(2), SourceBootId(1)), "register source 2");
    auto dom = eng.create_domain(DomainKind::NETWORK_PATH, cap100());
    chk(eng.publish_measurement(dom.id, meas(MeasurementKind::OFFERED_BYTES_PER_SEC, 90e6), WorkerId(1), WorkerBootId(1)),
        "worker 1 offered");
    chk(eng.publish_measurement(dom.id, meas(MeasurementKind::COMPLETED_BYTES_PER_SEC, 60e6), WorkerId(1), WorkerBootId(1)),
        "worker 1 serviced");
    chk(eng.recompute_domain(dom.id).state != CongestionState::UNKNOWN, "classified");
    chk(eng.fence_worker(WorkerId(1), WorkerBootId(1)), "fence worker 1");
    // Worker 2 publishes an unrelated latency measurement and starts a flow.
    chk(eng.publish_measurement(dom.id, meas(MeasurementKind::SERVICE_LATENCY, 0.001), WorkerId(2), WorkerBootId(1)),
        "latency published");
    FlowOptions fo;
    fo.owner_worker = WorkerId(2); fo.owner_boot = WorkerBootId(1);
    fo.source = SourceId(2); fo.source_boot = SourceBootId(1);
    fo.domain = dom; fo.traffic_class = TrafficClass::BULK_TRANSFER;
    fo.submitted_bytes = 1u << 20; fo.expected_bytes = 1u << 20;
    auto fl = eng.declare_flow(fo);
    chk(fl.id.is_valid() && eng.admit_flow(fl) && eng.start_flow(fl, WorkerBootId(1)), "worker 2 flow started");
    const CongestionAssessment a = eng.recompute_domain(dom.id);
    chk(a.state == CongestionState::REVALIDATION_REQUIRED && !a.authoritative,
        "still revalidation required without republished load");
    chk(eng.publish_measurement(dom.id, meas(MeasurementKind::OFFERED_BYTES_PER_SEC, 20e6), WorkerId(2), WorkerBootId(1)),
        "fresh offered load");
    chk(eng.recompute_domain(dom.id).state != CongestionState::REVALIDATION_REQUIRED, "revalidated by fresh load");
  }

  // A restart does not restore pre-restart offered/serviced load as current.
  {
    const char* path = "cf_regression_state.bin";
    {
      CongestionFabric eng;
      auto dom = eng.create_domain(DomainKind::NETWORK_PATH, cap100());
      chk(eng.publish_measurement(dom.id, meas(MeasurementKind::OFFERED_BYTES_PER_SEC, 120e6)), "offered");
      chk(eng.publish_measurement(dom.id, meas(MeasurementKind::COMPLETED_BYTES_PER_SEC, 40e6)), "serviced");
      chk(eng.recompute_domain(dom.id).state != CongestionState::UNKNOWN, "classified before restart");
      chk(eng.save(path), "save");
    }
    CongestionFabric eng2;
    chk(eng2.load(path), "load");
    std::remove(path);
    const auto ids = eng2.domain_ids();
    chk(ids.size() == 1, "domain restored");
    if (ids.size() == 1) {
      const CongestionDomainId d = ids[0];
      chk(eng2.snapshot(d).offered_bps == 0.0, "pre-restart offered load is not current");
      chk(eng2.publish_measurement(d, meas(MeasurementKind::COMPLETED_BYTES_PER_SEC, 100e6)), "fresh serviced");
      const CongestionAssessment a = eng2.recompute_domain(d);
      chk(a.state != CongestionState::SATURATED && a.state != CongestionState::CONGESTED &&
              a.state != CongestionState::SEVERELY_CONGESTED,
          "state not derived from pre-restart offered load");
    }
  }

  std::printf("cf_regression: %s (%d failures)\n", fails == 0 ? "OK" : "FAIL", fails);
  return fails == 0 ? 0 : 1;
}
