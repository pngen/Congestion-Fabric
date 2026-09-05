#include "congestion_fabric/engine.hpp"
#include <cstdio>
#include <cstring>
using namespace congfabric;

static int fails = 0;
static void chk(bool c, const char* m) {
  if (!c) { std::printf("FAIL: %s\n", m); ++fails; }
}
static Measurement meas(MeasurementKind k, double v, MeasurementProvenance p) {
  Measurement m; m.kind = k; m.value = v; m.provenance = p; m.confidence = 1.0; return m;
}

int main() {
  EngineLimits lim;
  CongestionFabric eng(lim);
  CapacityProfile cap;
  cap.set_theoretical(Rate(100e6, BandwidthKind::THEORETICAL, MeasurementProvenance::MEASURED));
  cap.set_configured(Rate(100e6, BandwidthKind::CONFIGURED, MeasurementProvenance::REPORTED));
  auto dom = eng.create_domain(DomainKind::NETWORK_PATH, cap);
  chk(dom.id.is_valid(), "domain created");
  chk(dom.generation.is_valid(), "domain generation present");

  chk(eng.register_worker(WorkerId(1), WorkerBootId(1)), "register worker boot1");
  chk(eng.register_source(SourceId(1), SourceBootId(1)), "register source boot1");
  // boot regression / duplicate rejected
  chk(!eng.register_worker(WorkerId(1), WorkerBootId(1)), "duplicate worker boot rejected");

  FlowOptions fo;
  fo.owner_worker = WorkerId(1); fo.owner_boot = WorkerBootId(1);
  fo.source = SourceId(1); fo.source_boot = SourceBootId(1);
  fo.domain = dom; fo.traffic_class = TrafficClass::BULK_TRANSFER;
  fo.submitted_bytes = 10u << 20; fo.expected_bytes = 10u << 20;
  auto fl = eng.declare_flow(fo);
  chk(fl.id.is_valid(), "flow declared");

  // A stale worker boot cannot declare.
  FlowOptions fo_stale = fo; fo_stale.owner_boot = WorkerBootId(999);
  chk(!eng.declare_flow(fo_stale).id.is_valid(), "stale worker declare rejected");

  chk(eng.admit_flow(fl), "admit");
  const Flow* fadm = eng.flow(fl.id);
  chk(fadm && fadm->admitted_bytes.value() == (10u << 20), "admitted == submitted");
  chk(eng.queue_flow(fl), "queue");
  chk(eng.start_flow(fl, WorkerBootId(1)), "start");
  const Flow* f = eng.flow(fl.id);
  chk(f && f->state == FlowState::ACTIVE, "state ACTIVE");
  chk(f && f->in_flight_bytes.value() == (10u << 20), "in_flight == admitted");

  // stale worker boot progress rejected
  chk(!eng.flow_progress(fl, WorkerBootId(999), 1u << 20), "stale progress rejected");
  // cannot complete from a non-live transition / double complete
  chk(eng.flow_progress(fl, WorkerBootId(1), 4u << 20), "progress 4MB");
  const Flow* f1 = eng.flow(fl.id);
  chk(f1 && f1->completed_bytes.value() == (4u << 20), "completed 4MB");
  chk(f1 && f1->in_flight_bytes.value() == (6u << 20), "in_flight 6MB");
  chk(eng.flow_complete(fl, WorkerBootId(1)), "complete");
  const Flow* f2 = eng.flow(fl.id);
  chk(f2 && f2->state == FlowState::COMPLETED, "state COMPLETED");
  chk(f2 && f2->completed_bytes.value() == (10u << 20), "completed all");
  chk(f2 && f2->in_flight_bytes.value() == 0, "in_flight drained");
  chk(!eng.flow_complete(fl, WorkerBootId(1)), "double complete rejected");
  chk(!eng.start_flow(fl, WorkerBootId(1)), "completed->active rejected");

  // Congestion classification with a still-active competing flow.
  FlowOptions fb = fo;
  fb.submitted_bytes = 50u << 20; fb.expected_bytes = 50u << 20;
  auto flb = eng.declare_flow(fb);
  chk(eng.admit_flow(flb), "admit B");
  chk(eng.start_flow(flb, WorkerBootId(1)), "start B");
  eng.publish_measurement(dom.id, meas(MeasurementKind::OFFERED_BYTES_PER_SEC, 120e6,
                                       MeasurementProvenance::REPORTED));
  eng.publish_measurement(dom.id, meas(MeasurementKind::COMPLETED_BYTES_PER_SEC, 40e6,
                                       MeasurementProvenance::REPORTED));
  eng.publish_measurement(dom.id, meas(MeasurementKind::SERVICE_LATENCY, 0.02,
                                       MeasurementProvenance::DERIVED));
  auto a = eng.recompute_domain(dom.id);
  chk(a.state == CongestionState::CONGESTED ||
          a.state == CongestionState::SEVERELY_CONGESTED ||
          a.state == CongestionState::QUEUEING ||
          a.state == CongestionState::SATURATED,
      "congested state observed");
  chk(a.authoritative, "assessment authoritative");
  DomainSnapshot snap = eng.snapshot(dom.id);
  chk(snap.state == a.state, "snapshot state matches");

  auto resc = eng.query_residual_capacity(dom.id, 120e6);
  chk(resc.has_capacity, "residual computed");
  chk(resc.residual_bps >= 0.0, "residual non-negative");

  auto fair = eng.evaluate_fairness(dom.id);
  chk(!fair.shares.empty(), "fairness report has shares");
  chk(fair.shares[0].target_share_bps >= 0.0, "fairness target non-negative");

  auto bp = eng.eval_backpressure(dom.id);
  chk(intent_is_restrictive(bp.intent), "backpressure restrictive");
  chk(eng.apply_local_shaping(dom.id, bp), "local shaping applied");

  std::printf("smoke: %s (%d failures)\n", fails == 0 ? "OK" : "FAIL", fails);
  return fails == 0 ? 0 : 1;
}
