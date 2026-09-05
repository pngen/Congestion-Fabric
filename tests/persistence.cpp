#include "congestion_fabric/engine.hpp"
#include <cstdio>
#include <fstream>
#include <vector>
#include <string>
using namespace congfabric;

static int fails = 0;
static void chk(bool c, const char* m) { if (!c) { std::printf("FAIL: %s\n", m); ++fails; } }

static bool write_all(const std::string& p, const std::vector<char>& b) {
  std::ofstream f(p, std::ios::binary);
  f.write(b.data(), static_cast<std::streamsize>(b.size()));
  return f.good();
}
static std::vector<char> read_all(const std::string& p) {
  std::ifstream f(p, std::ios::binary);
  return std::vector<char>((std::istreambuf_iterator<char>(f)),
                           std::istreambuf_iterator<char>());
}
static CongestionFabric build_engine() {
  CongestionFabric eng;
  CapacityProfile cap;
  cap.set_theoretical(Rate(50e6, BandwidthKind::THEORETICAL, MeasurementProvenance::MEASURED));
  cap.set_configured(Rate(50e6, BandwidthKind::CONFIGURED, MeasurementProvenance::REPORTED));
  auto dom = eng.create_domain(DomainKind::STORAGE_WRITE, cap);
  eng.register_worker(WorkerId(7), WorkerBootId(3));
  eng.register_source(SourceId(8), SourceBootId(4));
  FlowOptions fo;
  fo.owner_worker = WorkerId(7); fo.owner_boot = WorkerBootId(3);
  fo.source = SourceId(8); fo.source_boot = SourceBootId(4);
  fo.domain = dom; fo.traffic_class = TrafficClass::CHECKPOINT;
  fo.submitted_bytes = 1000; fo.expected_bytes = 1000;
  auto fl = eng.declare_flow(fo);
  eng.admit_flow(fl);
  eng.start_flow(fl, WorkerBootId(3));
  eng.flow_progress(fl, WorkerBootId(3), 400);
  eng.flow_complete(fl, WorkerBootId(3));
  return eng;
}

int main() {
  const std::string good = "pers_good.bin";
  const std::string corrupt = "pers_corrupt.bin";
  const std::string trunc = "pers_trunc.bin";
  const std::string garbage = "pers_garbage.bin";
  const std::string trailing = "pers_trailing.bin";

  {
    CongestionFabric eng = build_engine();
    chk(eng.save(good), "save ok");
  }
  {
    CongestionFabric eng;
    chk(eng.load(good), "load ok");
    chk(eng.domain_count() == 1, "one domain restored");
    chk(eng.epoch().value() == 1, "epoch restored");
    // find the flow
    auto ids = eng.domain_ids();
    chk(!ids.empty(), "domain id present");
    auto rs = eng.episode_history(ids[0]);
    (void)rs;
    std::uint64_t total = 0;
    // Determine flow via live_flow_count (should be 0 because flow completed)
    chk(eng.live_flow_count() == 0, "completed flow is not live");
    (void)total;
  }
  { // corruption: flip a payload byte
    auto b = read_all(good);
    chk(b.size() > 40, "good file large enough");
    std::size_t mid = b.size() / 2;  // always in bounds
    b[mid] ^= 0x55;
    chk(write_all(corrupt, b), "write corrupt");
    CongestionFabric eng;
    chk(!eng.load(corrupt), "corrupt payload rejected");
  }
  { // truncation
    auto b = read_all(good);
    b.resize(40);
    chk(write_all(trunc, b), "write trunc");
    CongestionFabric eng;
    chk(!eng.load(trunc), "truncated rejected");
  }
  { // bad magic
    std::vector<char> b(100, 0);
    chk(write_all(garbage, b), "write garbage");
    CongestionFabric eng;
    chk(!eng.load(garbage), "bad magic rejected");
  }
  { // trailing garbage
    auto b = read_all(good);
    b.push_back('x'); b.push_back('y');
    chk(write_all(trailing, b), "write trailing");
    CongestionFabric eng;
    chk(!eng.load(trailing), "trailing garbage rejected");
  }
  std::printf("persistence: %s (%d failures)\n", fails == 0 ? "OK" : "FAIL", fails);
  return fails == 0 ? 0 : 1;
}
