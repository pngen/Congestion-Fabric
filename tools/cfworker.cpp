// Congestion Fabric reference worker / controller client.
// Each invocation is an independent OS process talking framed TCP to the
// coordinator. Scenarios cover registration, domain creation, flow lifecycle,
// measurement publication, congestion querying, worker death/resurrection and
// clean shutdown.
#include "../include/congestion_fabric/net.hpp"
#include "../include/congestion_fabric/protocol.hpp"
#include "../include/congestion_fabric/persistence.hpp"
#include "../include/congestion_fabric/enums.hpp"
#include "../include/congestion_fabric/types.hpp"
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

using namespace congfabric;
using congfabric::persist::Reader;
using congfabric::persist::Writer;

namespace {

std::uint16_t g_port = 0;
std::string g_scenario;
std::uint64_t g_worker = 1, g_boot = 1, g_source = 1, g_sboot = 1;
std::uint64_t g_domain = 0, g_dgen = 0, g_bytes = 0, g_class = 0, g_delta = 0;
double g_offered = 0, g_serviced = 0, g_capacity = 0;
std::string g_idfile, g_path;
bool g_stay = false;

std::uint64_t rd_ll(const char* s) { return std::strtoull(s, nullptr, 10); }
double rd_d(const char* s) { return std::atof(s); }

void arg_scan(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    const char* a = argv[i];
    auto next = [&]() -> const char* { return (i + 1 < argc) ? argv[++i] : ""; };
    if (!std::strcmp(a, "--port")) g_port = static_cast<std::uint16_t>(rd_ll(next()));
    else if (!std::strcmp(a, "--scenario")) g_scenario = next();
    else if (!std::strcmp(a, "--worker")) g_worker = rd_ll(next());
    else if (!std::strcmp(a, "--boot")) g_boot = rd_ll(next());
    else if (!std::strcmp(a, "--source")) g_source = rd_ll(next());
    else if (!std::strcmp(a, "--sboot")) g_sboot = rd_ll(next());
    else if (!std::strcmp(a, "--domain")) g_domain = rd_ll(next());
    else if (!std::strcmp(a, "--dgen")) g_dgen = rd_ll(next());
    else if (!std::strcmp(a, "--bytes")) g_bytes = rd_ll(next());
    else if (!std::strcmp(a, "--delta")) g_delta = rd_ll(next());
    else if (!std::strcmp(a, "--class")) g_class = rd_ll(next());
    else if (!std::strcmp(a, "--offered")) g_offered = rd_d(next());
    else if (!std::strcmp(a, "--serviced")) g_serviced = rd_d(next());
    else if (!std::strcmp(a, "--capacity")) g_capacity = rd_d(next());
    else if (!std::strcmp(a, "--idfile")) g_idfile = next();
    else if (!std::strcmp(a, "--path")) g_path = next();
    else if (!std::strcmp(a, "--stay")) g_stay = true;
  }
}

bool recv_frame(SOCKET s, proto::Frame& out) {
  char hdr[proto::kHeaderBytes];
  if (!net::read_exact(s, hdr, proto::kHeaderBytes)) return false;
  if (std::memcmp(hdr, proto::kMagic, 4) != 0) return false;
  if (static_cast<std::uint8_t>(hdr[4]) != proto::kVersion) return false;
  std::uint8_t type = static_cast<std::uint8_t>(hdr[5]);
  std::uint32_t body_len = 0;
  for (int i = 0; i < 4; ++i) body_len |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(hdr[6 + i])) << (8 * i);
  if (body_len > proto::kMaxFrameBody) return false;
  std::vector<std::uint8_t> body(body_len);
  if (body_len && !net::read_exact(s, reinterpret_cast<char*>(body.data()), body_len)) return false;
  char cb[4];
  if (!net::read_exact(s, cb, 4)) return false;
  std::uint32_t stored = 0;
  for (int i = 0; i < 4; ++i) stored |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(cb[i])) << (8 * i);
  if (!proto::validate_crc(type, body.data(), body_len, stored)) return false;
  out.type = static_cast<proto::MessageType>(type);
  out.body = std::move(body);
  return true;
}

bool send_frame(SOCKET s, proto::MessageType t, const std::vector<std::uint8_t>& b) {
  proto::Frame f; f.type = t; f.body = b;
  auto v = proto::encode_frame(f);
  return net::write_all(s, reinterpret_cast<const char*>(v.data()), v.size());
}

bool hello(SOCKET s) {
  Writer b; b.str("cfworker");
  if (!send_frame(s, proto::MessageType::HELLO, b.data())) return false;
  proto::Frame f; return recv_frame(s, f);
}

bool do_register(SOCKET s) {
  Writer b; b.u64(g_worker); b.u64(g_boot); b.u64(g_source); b.u64(g_sboot);
  send_frame(s, proto::MessageType::REGISTER, b.data());
  proto::Frame f;
  if (!(recv_frame(s, f) && f.type == proto::MessageType::REGISTER_ACK)) return false;
  Reader r(f.body); return r.u8() != 0;
}

bool do_create(SOCKET s, std::uint64_t& id, std::uint64_t& gen) {
  Writer b; b.u8(static_cast<std::uint8_t>(DomainKind::NETWORK_PATH));
  b.u8(static_cast<std::uint8_t>(BandwidthKind::THEORETICAL)); b.f64(g_capacity);
  b.u8(static_cast<std::uint8_t>(BandwidthKind::CONFIGURED)); b.f64(g_capacity);
  send_frame(s, proto::MessageType::CREATE_DOMAIN, b.data());
  proto::Frame f;
  if (!(recv_frame(s, f) && f.type == proto::MessageType::DOMAIN_CREATED)) return false;
  Reader r(f.body);
  std::uint8_t ok = r.u8(); id = r.u64(); gen = r.u64();
  if (ok && !g_idfile.empty()) { std::ofstream out(g_idfile); out << id << " " << gen; }
  return ok != 0;
}

bool do_declare(SOCKET s, std::uint64_t did, std::uint64_t dgen,
                std::uint64_t& fid, std::uint64_t& fg) {
  Writer b; b.u64(g_worker); b.u64(g_boot); b.u64(g_source); b.u64(g_sboot);
  b.u8(static_cast<std::uint8_t>(g_class));
  b.u64(did); b.u64(dgen); b.u64(g_bytes); b.u64(g_bytes);
  b.f64(0); b.f64(0); b.f64(0); b.f64(0); b.f64(0);
  b.u8(static_cast<std::uint8_t>(MeasurementProvenance::REPORTED));
  send_frame(s, proto::MessageType::DECLARE_FLOW, b.data());
  proto::Frame f;
  if (!(recv_frame(s, f) && f.type == proto::MessageType::FLOW_DECLARED)) return false;
  Reader r(f.body); std::uint8_t ok = r.u8(); fid = r.u64(); fg = r.u64();
  return ok != 0;
}

bool send_flow_op(SOCKET s, proto::MessageType t, std::uint64_t fid, std::uint64_t fg) {
  Writer b; b.u64(fid); b.u64(fg);
  send_frame(s, t, b.data());
  proto::Frame f; return recv_frame(s, f) && f.type == proto::MessageType::OK;
}

bool do_start(SOCKET s, std::uint64_t fid, std::uint64_t fg) {
  Writer b; b.u64(fid); b.u64(fg); b.u64(g_boot);
  send_frame(s, proto::MessageType::START, b.data());
  proto::Frame f; return recv_frame(s, f) && f.type == proto::MessageType::OK;
}

void do_measure(SOCKET s, std::uint64_t did) {
  Writer b; b.u64(did);
  b.u8(static_cast<std::uint8_t>(MeasurementKind::OFFERED_BYTES_PER_SEC)); b.f64(g_offered);
  b.u8(static_cast<std::uint8_t>(MeasurementProvenance::REPORTED)); b.f64(1.0);
  send_frame(s, proto::MessageType::PUBLISH_MEASUREMENT, b.data());
  proto::Frame f; recv_frame(s, f);
  Writer b2; b2.u64(did);
  b2.u8(static_cast<std::uint8_t>(MeasurementKind::COMPLETED_BYTES_PER_SEC)); b2.f64(g_serviced);
  b2.u8(static_cast<std::uint8_t>(MeasurementProvenance::REPORTED)); b2.f64(1.0);
  send_frame(s, proto::MessageType::PUBLISH_MEASUREMENT, b2.data());
  proto::Frame f2; recv_frame(s, f2);
}

int do_query(SOCKET s, std::uint64_t did) {
  Writer b; b.u64(did);
  send_frame(s, proto::MessageType::QUERY_CONGESTION, b.data());
  proto::Frame f;
  if (!(recv_frame(s, f) && f.type == proto::MessageType::CONGESTION_UPDATE)) { std::printf("NO_UPDATE\n"); return 1; }
  Reader r(f.body);
  std::uint8_t st = r.u8(), prov = r.u8();
  double sc = r.f64(); std::uint8_t auth = r.u8(); std::uint8_t hasload = r.u8();
  double offered = r.f64(), serviced = r.f64(), eff = r.f64(), resid = r.f64();
  std::printf("CONGESTION state=%d prov=%d score=%f auth=%d hasload=%d offered=%f serviced=%f eff=%f resid=%f\n",
              (int)st, (int)prov, sc, (int)auth, (int)hasload, offered, serviced, eff, resid);
  return 0;
}

std::uint64_t get_domain() {
  std::ifstream in(g_idfile);
  std::uint64_t id = 0, gen = 0;
  if (in) in >> id >> gen;
  return id;
}

}  // namespace

int main(int argc, char** argv) {
  std::setvbuf(stdout, nullptr, _IONBF, 0);
  arg_scan(argc, argv);
  if (!net::init()) { std::fprintf(stderr, "worker: winsock init failed\n"); return 2; }
  SOCKET s = net::connect_socket(g_port);
  if (s == INVALID_SOCKET) { std::fprintf(stderr, "worker: connect failed\n"); net::cleanup(); return 2; }
  if (!hello(s)) { std::fprintf(stderr, "worker: hello failed\n"); net::cleanup(); return 2; }

  int rc = 0;
  if (g_scenario == "register") rc = do_register(s) ? 0 : 1;
  else if (g_scenario == "create") { std::uint64_t id, gen; rc = do_create(s, id, gen) ? 0 : 1; }
  else if (g_scenario == "declare") { std::uint64_t fid, fg; rc = do_declare(s, g_domain, g_dgen, fid, fg) ? 0 : 1; }
  else if (g_scenario == "admit") rc = send_flow_op(s, proto::MessageType::ADMIT, g_domain, g_dgen) ? 0 : 1;
  else if (g_scenario == "start") rc = do_start(s, g_domain, g_dgen) ? 0 : 1;
  else if (g_scenario == "progress") { Writer b; b.u64(g_domain); b.u64(g_dgen); b.u64(g_boot); b.u64(g_delta); send_frame(s, proto::MessageType::PROGRESS, b.data()); proto::Frame f; rc = (recv_frame(s, f) && f.type == proto::MessageType::OK) ? 0 : 1; }
  else if (g_scenario == "complete") { Writer b; b.u64(g_domain); b.u64(g_dgen); b.u64(g_boot); send_frame(s, proto::MessageType::COMPLETE, b.data()); proto::Frame f; rc = (recv_frame(s, f) && f.type == proto::MessageType::OK) ? 0 : 1; }
  else if (g_scenario == "measure") { do_measure(s, g_domain); rc = 0; }
  else if (g_scenario == "query") rc = do_query(s, g_domain);
  else if (g_scenario == "drive") {
    if (!do_register(s)) { std::printf("DRIVE_FAIL register\n"); rc = 1; }
    else {
      std::uint64_t did, dgen, fid, fg;
      if (!do_create(s, did, dgen)) { std::printf("DRIVE_FAIL create\n"); rc = 1; }
      else {
        std::printf("DRIVE create domain=%llu gen=%llu\n", (unsigned long long)did, (unsigned long long)dgen);
        if (!do_declare(s, did, dgen, fid, fg)) { std::printf("DRIVE_FAIL declare\n"); rc = 1; }
        else if (!send_flow_op(s, proto::MessageType::ADMIT, fid, fg)) { std::printf("DRIVE_FAIL admit\n"); rc = 1; }
        else if (!do_start(s, fid, fg)) { std::printf("DRIVE_FAIL start\n"); rc = 1; }
        else {
          do_measure(s, did);
          std::printf("DRIVE_OK domain=%llu flow=%llu\n", (unsigned long long)did, (unsigned long long)fid);
          if (g_stay) { proto::Frame f; while (recv_frame(s, f)) {} }
        }
      }
    }
  }
  else if (g_scenario == "compete") {
    if (!do_register(s)) { rc = 1; }
    else {
      std::uint64_t fid, fg;
      std::uint64_t did = get_domain();
      if (!do_declare(s, did, 1, fid, fg)) { std::printf("COMPETE_FAIL declare\n"); rc = 1; }
      else if (!send_flow_op(s, proto::MessageType::ADMIT, fid, fg)) { std::printf("COMPETE_FAIL admit\n"); rc = 1; }
      else if (!do_start(s, fid, fg)) { std::printf("COMPETE_FAIL start\n"); rc = 1; }
      else {
        do_measure(s, did);
        std::printf("COMPETE_OK domain=%llu flow=%llu\n", (unsigned long long)did, (unsigned long long)fid);
        rc = do_query(s, did);
        if (g_stay) { proto::Frame f; while (recv_frame(s, f)) {} }
      }
    }
  }
  else if (g_scenario == "resurrect") {
    if (!do_register(s)) { rc = 1; }
    else {
      std::uint64_t did = get_domain();
      std::uint64_t fid, fg;
      if (!do_declare(s, did, 1, fid, fg)) { std::printf("RESURRECT_FAIL declare\n"); rc = 1; }
      else if (!send_flow_op(s, proto::MessageType::ADMIT, fid, fg)) { std::printf("RESURRECT_FAIL admit\n"); rc = 1; }
      else if (!do_start(s, fid, fg)) { std::printf("RESURRECT_FAIL start\n"); rc = 1; }
      else { do_measure(s, did); std::printf("RESURRECT_OK flow=%llu\n", (unsigned long long)fid); rc = 0; }
    }
  }
  else if (g_scenario == "control") {
    std::uint64_t did = (g_domain != 0) ? g_domain : get_domain();
    if (did == 0) { std::printf("CONTROL no domain\n"); rc = 1; }
    else {
      rc = do_query(s, did);
      if (!g_path.empty()) { Writer b; b.str(g_path); send_frame(s, proto::MessageType::SAVE, b.data()); proto::Frame f; recv_frame(s, f); }
      proto::Frame f;
      send_frame(s, proto::MessageType::SHUTDOWN, {});
      recv_frame(s, f);
      std::printf("CONTROL_DONE state=%d\n", rc);
    }
  }
  else if (g_scenario == "replay") {
    // Stale Worker A authority replay. Register with the OLD (already fenced)
    // boot, then attempt live measurement + progress + completion against the
    // now-stale flow. Every operation must be rejected before touching current
    // state.
    bool register_rejected = false;
    { Writer b; b.u64(g_worker); b.u64(g_boot); b.u64(g_source); b.u64(g_sboot);
      send_frame(s, proto::MessageType::REGISTER, b.data());
      proto::Frame f; recv_frame(s, f);
      if (f.type == proto::MessageType::REGISTER_ACK) {
        Reader r(f.body); std::uint8_t ack = r.u8();
        register_rejected = (ack == 0);
      } else register_rejected = true;
    }
    bool measure_rejected = false;
    { Writer b; b.u64(g_domain);
      b.u8(static_cast<std::uint8_t>(MeasurementKind::OFFERED_BYTES_PER_SEC)); b.f64(g_offered);
      b.u8(static_cast<std::uint8_t>(MeasurementProvenance::REPORTED)); b.f64(1.0);
      send_frame(s, proto::MessageType::PUBLISH_MEASUREMENT, b.data());
      proto::Frame f; measure_rejected = (recv_frame(s, f) && f.type == proto::MessageType::NACK);
    }
    bool progress_rejected = false;
    { Writer b; b.u64(g_domain); b.u64(g_dgen); b.u64(g_boot); b.u64(g_delta);
      send_frame(s, proto::MessageType::PROGRESS, b.data());
      proto::Frame f; progress_rejected = (recv_frame(s, f) && f.type == proto::MessageType::NACK);
    }
    bool complete_rejected = false;
    { Writer b; b.u64(g_domain); b.u64(g_dgen); b.u64(g_boot);
      send_frame(s, proto::MessageType::COMPLETE, b.data());
      proto::Frame f; complete_rejected = (recv_frame(s, f) && f.type == proto::MessageType::NACK);
    }
    bool all = register_rejected && measure_rejected && progress_rejected && complete_rejected;
    std::printf("REPLAY register_rejected=%d measure_nack=%d progress_nack=%d complete_nack=%d all_rejected=%d\n",
                register_rejected ? 1 : 0, measure_rejected ? 1 : 0, progress_rejected ? 1 : 0,
                complete_rejected ? 1 : 0, all ? 1 : 0);
    rc = all ? 0 : 1;
  }
  else if (g_scenario == "shutdown") { send_frame(s, proto::MessageType::SHUTDOWN, {}); proto::Frame f; recv_frame(s, f); rc = 0; }
  else if (g_scenario == "fence") { Writer b; b.u64(g_worker); b.u64(g_boot); send_frame(s, proto::MessageType::FENCE_WORKER, b.data()); proto::Frame f; rc = (recv_frame(s, f) && f.type == proto::MessageType::OK) ? 0 : 1; }
  else if (g_scenario == "save") { Writer b; b.str(g_path); send_frame(s, proto::MessageType::SAVE, b.data()); proto::Frame f; rc = (recv_frame(s, f) && f.type == proto::MessageType::OK) ? 0 : 1; }
  else { std::fprintf(stderr, "unknown scenario\n"); rc = 2; }

  net::close_socket(s);
  net::cleanup();
  return rc;
}
