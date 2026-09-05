// Independent downstream consumer (find_package).
#include <congestion_fabric/engine.hpp>
#include <cstdio>
using namespace congfabric;
int main() {
  CongestionFabric eng;
  CapacityProfile cap;
  cap.set_theoretical(Rate(100e6, BandwidthKind::THEORETICAL, MeasurementProvenance::REPORTED));
  cap.set_configured(Rate(100e6, BandwidthKind::CONFIGURED, MeasurementProvenance::REPORTED));
  auto dom = eng.create_domain(DomainKind::NIC_TX, cap);
  if (!dom.id.is_valid()) { std::printf("FAIL: domain\n"); return 1; }
  eng.register_worker(WorkerId(1), WorkerBootId(1));
  eng.register_source(SourceId(1), SourceBootId(1));
  FlowOptions o;
  o.owner_worker = WorkerId(1); o.owner_boot = WorkerBootId(1);
  o.source = SourceId(1); o.source_boot = SourceBootId(1);
  o.domain = dom; o.traffic_class = TrafficClass::BULK_TRANSFER; o.submitted_bytes = 100;
  auto fl = eng.declare_flow(o);
  eng.admit_flow(fl);
  eng.start_flow(fl, WorkerBootId(1));
  Measurement mo; mo.kind = MeasurementKind::OFFERED_BYTES_PER_SEC; mo.value = 130e6;
  mo.provenance = MeasurementProvenance::REPORTED;
  eng.publish_measurement(dom.id, mo);
  Measurement ms; ms.kind = MeasurementKind::COMPLETED_BYTES_PER_SEC; ms.value = 60e6;
  ms.provenance = MeasurementProvenance::REPORTED;
  eng.publish_measurement(dom.id, ms);
  CongestionAssessment a = eng.assess_congestion(dom.id);
  BottleneckResult b = eng.identify_bottleneck({dom.id});
  std::printf("DOWNSTREAM: state=%d bottleneck_valid=%d residual=%f\n",
              (int)a.state, b.domain.id.is_valid() ? 1 : 0,
              eng.query_residual_capacity(dom.id, 130e6).residual_bps);
  return (a.state != CongestionState::UNKNOWN && b.domain.id == dom.id) ? 0 : 1;
}
