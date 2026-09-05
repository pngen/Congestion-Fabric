#include "congestion_fabric/checked.hpp"
#include "congestion_fabric/engine.hpp"
#include <cstdio>
#include <limits>
using namespace congfabric;

static int fails = 0;
static void chk(bool c, const char* m) { if (!c) { std::printf("FAIL: %s\n", m); ++fails; } }

int main() {
  { // checked arithmetic
    std::uint64_t o = 0;
    chk(!checked::add(std::numeric_limits<std::uint64_t>::max(), 1, o), "add overflow rejected");
    chk(checked::add(10, 5, o) && o == 15, "add ok");
    chk(!checked::sub(5, 10, o), "sub underflow rejected");
    chk(checked::sub(10, 5, o) && o == 5, "sub ok");
    std::uint64_t p = 0;
    chk(!checked::mul(1ULL << 32, 1ULL << 32, p), "mul overflow rejected (2^64)");
    chk(checked::mul(1000, 1000, p) && p == 1000000, "mul ok");
  }
  {
    CongestionFabric eng;
    CapacityProfile cap;
    cap.set_theoretical(Rate(100e6, BandwidthKind::THEORETICAL, MeasurementProvenance::MEASURED));
    auto dom = eng.create_domain(DomainKind::HOST_MEMORY_BANDWIDTH, cap);
    eng.register_worker(WorkerId(1), WorkerBootId(1));
    eng.register_source(SourceId(1), SourceBootId(1));
    FlowOptions fo;
    fo.owner_worker = WorkerId(1); fo.owner_boot = WorkerBootId(1);
    fo.source = SourceId(1); fo.source_boot = SourceBootId(1);
    fo.domain = dom; fo.submitted_bytes = 100;
    auto fl = eng.declare_flow(fo);
    chk(eng.admit_flow(fl), "admit");
    chk(eng.start_flow(fl, WorkerBootId(1)), "start");
    // progress cannot exceed admitted
    chk(!eng.flow_progress(fl, WorkerBootId(1), 101), "progress beyond admitted rejected");
    chk(eng.flow_progress(fl, WorkerBootId(1), 60), "progress 60");
    // cancel exactly the remaining, then cannot complete
    chk(eng.flow_cancel(fl, WorkerBootId(1), 40), "cancel 40 (remaining)");
    const Flow* f = eng.flow(fl.id);
    chk(f && f->state == FlowState::CANCELLED, "state CANCELLED");
    chk(f && f->completed_bytes.value() + f->cancelled_bytes.value() == 100,
        "completed+cancelled == admitted");
    chk(!eng.flow_complete(fl, WorkerBootId(1)), "cancelled cannot complete");
    // cancellation cannot resurrect after coordinator restart semantics
    chk(!eng.flow_cancel(fl, WorkerBootId(1), 40), "double cancel rejected");

    // stale generation cannot mutate current accounting
    FlowOptions fo2 = fo; fo2.submitted_bytes = 50;
    auto fl2 = eng.declare_flow(fo2);
    chk(eng.admit_flow(fl2), "admit2");
    chk(eng.advance_flow_generation(fl2, FlowGeneration(2)), "advance generation");
    chk(!eng.flow_progress(fl2, WorkerBootId(1), 1), "stale generation progress rejected");
    chk(eng.flow(fl2.id) && eng.flow(fl2.id)->ref.generation.value() == 2, "current gen is 2");
  }
  std::printf("accounting: %s (%d failures)\n", fails == 0 ? "OK" : "FAIL", fails);
  return fails == 0 ? 0 : 1;
}
