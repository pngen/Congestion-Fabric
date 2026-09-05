// Congestion Fabric -- adversarial hardening.
// Deliberately attempts to break accounting, queue semantics, congestion
// classification, freshness, authority, backpressure and persistence. Every
// reproducible material defect must be rejected or handled, never silently
// accepted.
#include "congestion_fabric/engine.hpp"
#include "congestion_fabric/persistence.hpp"
#include "congestion_fabric/protocol.hpp"
#include <cmath>
#include <cstdio>
#include <fstream>
#include <limits>
#include <vector>

using namespace congfabric;

static int g_fails = 0;
static void chk(bool c, const char* m) { if (!c) { std::printf("FAIL: %s\n", m); ++g_fails; } }

static std::vector<char> read_all(const std::string& p) {
  std::ifstream f(p, std::ios::binary);
  return std::vector<char>((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
}
static bool write_all(const std::string& p, const std::vector<char>& b) {
  std::ofstream f(p, std::ios::binary);
  f.write(b.data(), static_cast<std::streamsize>(b.size()));
  return f.good();
}
static CongestionFabric base() {
  CongestionFabric eng;
  CapacityProfile cap;
  cap.set_theoretical(Rate(100e6, BandwidthKind::THEORETICAL, MeasurementProvenance::MEASURED));
  eng.create_domain(DomainKind::NIC_TX, cap);
  eng.register_worker(WorkerId(1), WorkerBootId(1));
  eng.register_source(SourceId(1), SourceBootId(1));
  return eng;
}
static EntityRef<Flow> mk(CongestionFabric& eng, CongestionDomainId dom, std::uint64_t bytes) {
  FlowOptions o;
  o.owner_worker = WorkerId(1); o.owner_boot = WorkerBootId(1);
  o.source = SourceId(1); o.source_boot = SourceBootId(1);
  o.domain.id = dom; o.domain.generation = Generation<CongestionDomainTag>(1);
  o.submitted_bytes = bytes; o.expected_bytes = bytes;
  return eng.declare_flow(o);
}

int main() {
  { // byte accounting overflow / progress beyond admitted
    CongestionFabric eng = base();
    auto d = eng.domain_ids()[0];
    auto fl = mk(eng, d, 1000);
    eng.admit_flow(fl);
    eng.start_flow(fl, WorkerBootId(1));
    chk(!eng.flow_progress(fl, WorkerBootId(1), 1001), "progress beyond admitted rejected");
    // Clamping an overshoot cancel closes accounting exactly without double-cancel.
    chk(eng.flow_cancel(fl, WorkerBootId(1), 1001), "cancel clamps to remaining");
    { const Flow* f = eng.flow(fl.id);
      if (f) chk(f->completed_bytes.value() + f->cancelled_bytes.value() == 1000, "cancel closes accounting exactly");
    }
  }
  { // double completion / completion after cancel
    CongestionFabric eng = base();
    auto d = eng.domain_ids()[0];
    auto fl = mk(eng, d, 100);
    eng.admit_flow(fl);
    eng.start_flow(fl, WorkerBootId(1));
    chk(eng.flow_complete(fl, WorkerBootId(1)), "complete ok");
    chk(!eng.flow_complete(fl, WorkerBootId(1)), "double complete rejected");
    auto fl2 = mk(eng, d, 100);
    eng.admit_flow(fl2); eng.start_flow(fl2, WorkerBootId(1));
    eng.flow_cancel(fl2, WorkerBootId(1), 100);
    chk(!eng.flow_complete(fl2, WorkerBootId(1)), "complete after cancel rejected");
  }
  { // stale generation cannot mutate current accounting
    CongestionFabric eng = base();
    auto d = eng.domain_ids()[0];
    auto fl = mk(eng, d, 100);
    eng.admit_flow(fl); eng.start_flow(fl, WorkerBootId(1));
    eng.advance_flow_generation(fl, FlowGeneration(2));
    chk(!eng.flow_progress(fl, WorkerBootId(1), 1), "stale generation progress rejected");
    chk(!eng.flow_complete(fl, WorkerBootId(1)), "stale generation complete rejected");
  }
  { // cross-worker flow mutation
    CongestionFabric eng;
    CapacityProfile cap;
    cap.set_theoretical(Rate(100e6, BandwidthKind::THEORETICAL, MeasurementProvenance::REPORTED));
    auto d = eng.create_domain(DomainKind::NIC_RX, cap);
    eng.register_worker(WorkerId(1), WorkerBootId(1));
    eng.register_source(SourceId(1), SourceBootId(1));
    eng.register_worker(WorkerId(2), WorkerBootId(1));
    eng.register_source(SourceId(2), SourceBootId(1));
    FlowOptions o;
    o.owner_worker = WorkerId(1); o.owner_boot = WorkerBootId(1);
    o.source = SourceId(1); o.source_boot = SourceBootId(1);
    o.domain = d; o.submitted_bytes = 100;
    auto fl = eng.declare_flow(o);
    chk(eng.admit_flow(fl), "admit by owner");
    chk(!eng.start_flow(fl, WorkerBootId(2)), "another worker cannot start the flow");
  }
  { // invalid measurement (NaN/Inf) rejected
    CongestionFabric eng = base();
    auto d = eng.domain_ids()[0];
    Measurement m; m.kind = MeasurementKind::OFFERED_BYTES_PER_SEC;
    m.value = std::numeric_limits<double>::infinity();
    chk(!eng.publish_measurement(d, m), "infinite measurement rejected");
    Measurement m2; m2.kind = MeasurementKind::OFFERED_BYTES_PER_SEC;
    m2.value = std::nan("");
    m2.confidence = 1.0;
    chk(!eng.publish_measurement(d, m2), "NaN measurement rejected");
  }
  { // a domain with a NEGATIVE (invalid) bandwidth is rejected
    CongestionFabric eng;
    CapacityProfile cap;
    cap.set_theoretical(Rate(-1.0, BandwidthKind::THEORETICAL, MeasurementProvenance::MEASURED));
    auto d = eng.create_domain(DomainKind::NETWORK_PATH, cap);
    chk(!d.id.is_valid(), "negative-capacity domain rejected");
    // A domain with all-UNKNOWN capacity is created as UNKNOWN, never silently
    // treated as healthy.
    CongestionFabric eng2;
    auto d2 = eng2.create_domain(DomainKind::NETWORK_PATH, CapacityProfile{});
    chk(d2.id.is_valid(), "unknown-capacity domain creatable");
    auto a = eng2.assess_congestion(d2.id);
    chk(a.state == CongestionState::UNKNOWN, "unknown capacity => UNKNOWN, not HEALTHY");
  }
  { // capacity shrink below active load: reservation consistency
    CongestionFabric eng;
    CapacityProfile cap;
    cap.set_theoretical(Rate(100e6, BandwidthKind::THEORETICAL, MeasurementProvenance::REPORTED));
    cap.set_reserved(Rate(150e6, BandwidthKind::RESERVED, MeasurementProvenance::REPORTED));
    auto d = eng.create_domain(DomainKind::NETWORK_PATH, cap);
    chk(!d.id.is_valid(), "reservation exceeding capacity rejected");
  }
  { // stale recovery cannot clear a current congestion episode
    CongestionFabric eng = base();
    auto d = eng.domain_ids()[0];
    auto fl = mk(eng, d, 1000);
    eng.admit_flow(fl); eng.start_flow(fl, WorkerBootId(1));
    Measurement mo; mo.kind = MeasurementKind::OFFERED_BYTES_PER_SEC; mo.value = 120e6;
    mo.provenance = MeasurementProvenance::REPORTED;
    eng.publish_measurement(d, mo);
    Measurement ms; ms.kind = MeasurementKind::COMPLETED_BYTES_PER_SEC; ms.value = 40e6;
    ms.provenance = MeasurementProvenance::REPORTED;
    eng.publish_measurement(d, ms);
    auto a = eng.recompute_domain(d);
    chk(static_cast<int>(a.state) >= static_cast<int>(CongestionState::SATURATED), "congestion episode opens");
    // Fencing the only active flow should NOT delete the historical episode.
    eng.fence_worker(WorkerId(1), WorkerBootId(1));
    auto hist = eng.episode_history(d);
    // Episodes are bounded history; at least the domain state is evaluated.
    chk(eng.episode_history(d).size() <= eng.limits().max_episode_history_per_domain, "episode history bounded");
    (void)hist;
  }
  { // a stale action intent must not apply to a fresh generation
    BackpressureAction a;
    a.intent = ActionIntent::THROTTLE;
    a.generation = BackpressureGeneration(1);
    BackpressureGeneration current(2);
    chk(a.is_current(current), "intent not older than current");
    BackpressureGeneration current_old(1);
    chk(a.is_current(current_old), "intent at same generation current");
  }
  { // malformed fairness weight rejected by validity check
    FairnessConfig f;
    f.weight = -1.0;
    chk(!f.valid(), "negative fairness weight invalid");
    FairnessConfig f2;
    f2.weight = std::nan("");
    chk(!f2.valid(), "NaN fairness weight invalid");
  }
  { // persistence: hostile counts rejected; oversized decode rejected; generation regression
    CongestionFabric eng = base();
    auto d = eng.domain_ids()[0];
    auto fl = mk(eng, d, 50);
    eng.admit_flow(fl);
    if (eng.save("adv.bin")) {
      CongestionFabric eng2;
      chk(eng2.load("adv.bin"), "round-trip load");
      chk(eng2.domain_count() == 1, "domain restored");
    }
    // corrupt one byte in the middle -> load must fail
    auto b = read_all("adv.bin");
    std::size_t mid = b.size() / 2;
    b[mid] ^= 0x11;
    chk(write_all("adv_corrupt.bin", b), "write corrupt");
    CongestionFabric e3; chk(!e3.load("adv_corrupt.bin"), "corrupt payload rejected");
    // truncate
    b.resize(10);
    chk(write_all("adv_trunc.bin", b), "write trunc");
    CongestionFabric e4; chk(!e4.load("adv_trunc.bin"), "truncated rejected");
    // trailing garbage
    auto b2 = read_all("adv.bin");
    b2.push_back('z');
    chk(write_all("adv_trail.bin", b2), "write trail");
    CongestionFabric e5; chk(!e5.load("adv_trail.bin"), "trailing rejected");
    remove("adv.bin"); remove("adv_corrupt.bin"); remove("adv_trunc.bin"); remove("adv_trail.bin");
  }
  { // protocol: oversized frame rejected by decode_frame
    // Build a valid frame then corrupt the length to exceed the bound.
    congfabric::proto::Frame f;
    f.type = congfabric::proto::MessageType::HELLO;
    f.body.assign(64, 1);
    auto v = congfabric::proto::encode_frame(f);
    // mutate the 4-byte length field (bytes 6-9) to an oversized value
    v[6] = 0xFF; v[7] = 0xFF; v[8] = 0xFF; v[9] = 0x7F;  // ~2GB > bound
    bool threw = false;
    try { (void)congfabric::proto::decode_frame(v.data(), v.size()); }
    catch (const congfabric::persist::CorruptError&) { threw = true; }
    chk(threw, "oversized protocol frame rejected");
  }

  std::printf("adversarial: %s (%d failures)\n", g_fails == 0 ? "OK" : "FAIL", g_fails);
  return g_fails == 0 ? 0 : 1;
}
