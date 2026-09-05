// Congestion Fabric reference coordinator.
// Runs the authoritative CongestionFabric engine behind the framed TCP protocol,
// serves workers, and persists state for restart recovery.
#include "congestion_fabric/engine.hpp"
#include "congestion_fabric/net.hpp"
#include "congestion_fabric/protocol.hpp"
#include "congestion_fabric/types.hpp"
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace congfabric;
using congfabric::persist::Reader;
using congfabric::persist::Writer;

namespace {

std::atomic<bool> g_stop{false};

// Track live client sockets and the listener so shutdown can unblock sessions
// instead of waiting forever on a join.
std::mutex g_sock_mtx;
std::vector<SOCKET> g_socks;
SOCKET g_listener = INVALID_SOCKET;

void reg_sock(SOCKET s) { std::lock_guard<std::mutex> lk(g_sock_mtx); g_socks.push_back(s); }
void unreg_sock(SOCKET s) {
  std::lock_guard<std::mutex> lk(g_sock_mtx);
  g_socks.erase(std::remove(g_socks.begin(), g_socks.end(), s), g_socks.end());
}
void close_other_socks(SOCKET keep) {
  std::lock_guard<std::mutex> lk(g_sock_mtx);
  for (SOCKET x : g_socks) if (x != keep) net::close_socket(x);
}

bool recv_frame(SOCKET s, proto::Frame& out) {
  char hdr[proto::kHeaderBytes];
  if (!net::read_exact(s, hdr, proto::kHeaderBytes)) { std::fprintf(stderr, "recv: hdr fail\n"); return false; }
  if (std::memcmp(hdr, proto::kMagic, 4) != 0) { std::fprintf(stderr, "recv: bad magic %02x%02x%02x%02x\n", (unsigned char)hdr[0],(unsigned char)hdr[1],(unsigned char)hdr[2],(unsigned char)hdr[3]); return false; }
  std::uint8_t ver = static_cast<std::uint8_t>(hdr[4]);
  std::uint8_t type = static_cast<std::uint8_t>(hdr[5]);
  std::uint32_t body_len = 0;
  for (int i = 0; i < 4; ++i) body_len |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(hdr[6 + i])) << (8 * i);
  if (body_len > proto::kMaxFrameBody) { std::fprintf(stderr, "recv: oversized len %u\n", (unsigned)body_len); return false; }
  std::vector<std::uint8_t> body(body_len);
  if (body_len && !net::read_exact(s, reinterpret_cast<char*>(body.data()), body_len))
    { std::fprintf(stderr, "recv: body fail\n"); return false; }
  char crc_buf[4];
  if (!net::read_exact(s, crc_buf, 4)) { std::fprintf(stderr, "recv: crc read fail\n"); return false; }
  std::uint32_t stored = 0;
  for (int i = 0; i < 4; ++i) stored |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(crc_buf[i])) << (8 * i);
  std::uint32_t expect = proto::body_crc(type, body.data(), body_len);
  if (!proto::validate_crc(type, body.data(), body_len, stored)) {
    std::fprintf(stderr, "recv: crc mismatch type=%u len=%u stored=%08x expect=%08x bytes=", (unsigned)type, (unsigned)body_len, (unsigned)stored, (unsigned)expect);
    for (int i = 0; i < proto::kHeaderBytes; ++i) std::fprintf(stderr, "%02x", (unsigned char)hdr[i]);
    std::fprintf(stderr, "|");
    for (std::size_t i = 0; i < body.size(); ++i) std::fprintf(stderr, "%02x", body[i]);
    std::fprintf(stderr, "|%02x%02x%02x%02x\n", (unsigned char)crc_buf[0], (unsigned char)crc_buf[1], (unsigned char)crc_buf[2], (unsigned char)crc_buf[3]);
    return false;
  }
  out.type = static_cast<proto::MessageType>(type);
  out.body = std::move(body);
  return true;
}

bool send_frame(SOCKET s, proto::MessageType type, const std::vector<std::uint8_t>& body) {
  proto::Frame f;
  f.type = type;
  f.body = body;
  auto bytes = proto::encode_frame(f);
  return net::write_all(s, reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

void ok(SOCKET s) { send_frame(s, proto::MessageType::OK, {}); }
void nack(SOCKET s) { send_frame(s, proto::MessageType::NACK, {}); }

std::uint64_t rd_u64(Reader& r) { return r.u64(); }
std::uint64_t rd_u64_if(Reader& r) { return r.u64(); }

void handle(SOCKET s, CongestionFabric& eng, proto::Frame& f, WorkerId& ctx_wid,
            WorkerBootId& ctx_boot) {
  try {
    Reader r(f.body);
    switch (f.type) {
      case proto::MessageType::HELLO: {
        std::string name = r.str();
        (void)name;
        ok(s);
        break;
      }
      case proto::MessageType::REGISTER: {
        WorkerId w(r.u64());
        WorkerBootId b(r.u64());
        SourceId s1(r.u64());
        SourceBootId s2(r.u64());
        ctx_wid = w;
        ctx_boot = b;
        bool a = eng.register_worker(w, b);
        bool c = eng.register_source(s1, s2);
        (void)a; (void)c;
        Writer body;
        body.u8(a && c ? 1 : 0);
        send_frame(s, proto::MessageType::REGISTER_ACK, body.data());
        break;
      }
      case proto::MessageType::CREATE_DOMAIN: {
        DomainKind kind = static_cast<DomainKind>(r.u8());
        BandwidthKind tk = static_cast<BandwidthKind>(r.u8());
        double tbps = r.f64();
        BandwidthKind ck = static_cast<BandwidthKind>(r.u8());
        double cbps = r.f64();
        CapacityProfile cap;
        cap.set_theoretical(Rate(tbps, tk, MeasurementProvenance::MEASURED));
        cap.set_configured(Rate(cbps, ck, MeasurementProvenance::REPORTED));
        auto dom = eng.create_domain(kind, cap);
        Writer body;
        body.u8(dom.id.is_valid() ? 1 : 0);
        body.u64(dom.id.value());
        body.u64(dom.generation.value());
        send_frame(s, proto::MessageType::DOMAIN_CREATED, body.data());
        break;
      }
      case proto::MessageType::DECLARE_FLOW: {
        FlowOptions o;
        o.owner_worker = WorkerId(r.u64());
        o.owner_boot = WorkerBootId(r.u64());
        o.source = SourceId(r.u64());
        o.source_boot = SourceBootId(r.u64());
        o.traffic_class = static_cast<TrafficClass>(r.u8());
        auto did = CongestionDomainId(r.u64());
        auto dgen = Generation<CongestionDomainTag>(r.u64());
        o.domain.id = did;
        o.domain.generation = dgen;
        o.expected_bytes = r.u64();
        o.submitted_bytes = r.u64();
        o.priority = r.f64();
        o.latency_sensitivity = r.f64();
        o.throughput_target_bps = r.f64();
        o.burst_allowance_bytes = r.f64();
        o.deadline_ms = r.f64();
        o.provenance = static_cast<MeasurementProvenance>(r.u8());
        auto fl = eng.declare_flow(o);
        Writer body;
        body.u8(fl.id.is_valid() ? 1 : 0);
        body.u64(fl.id.value());
        body.u64(fl.generation.value());
        send_frame(s, proto::MessageType::FLOW_DECLARED, body.data());
        break;
      }
      case proto::MessageType::ADMIT: {
        EntityRef<Flow> ref;
        ref.id = FlowId(r.u64());
        ref.generation = FlowGeneration(r.u64());
        bool okres = eng.admit_flow(ref);
        (void)okres;
        if (okres) ok(s); else nack(s);
        break;
      }
      case proto::MessageType::QUEUE: {
        EntityRef<Flow> ref;
        ref.id = FlowId(r.u64());
        ref.generation = FlowGeneration(r.u64());
        if (eng.queue_flow(ref)) ok(s); else nack(s);
        break;
      }
      case proto::MessageType::START: {
        EntityRef<Flow> ref;
        ref.id = FlowId(r.u64());
        ref.generation = FlowGeneration(r.u64());
        WorkerBootId b(r.u64());
        if (eng.start_flow(ref, b)) ok(s); else nack(s);
        break;
      }
      case proto::MessageType::PROGRESS: {
        EntityRef<Flow> ref;
        ref.id = FlowId(r.u64());
        ref.generation = FlowGeneration(r.u64());
        WorkerBootId b(r.u64());
        std::uint64_t delta = r.u64();
        if (eng.flow_progress(ref, b, delta)) ok(s); else nack(s);
        break;
      }
      case proto::MessageType::COMPLETE: {
        EntityRef<Flow> ref;
        ref.id = FlowId(r.u64());
        ref.generation = FlowGeneration(r.u64());
        WorkerBootId b(r.u64());
        if (eng.flow_complete(ref, b)) ok(s); else nack(s);
        break;
      }
      case proto::MessageType::CANCEL: {
        EntityRef<Flow> ref;
        ref.id = FlowId(r.u64());
        ref.generation = FlowGeneration(r.u64());
        WorkerBootId b(r.u64());
        std::uint64_t cancelled = r.u64();
        if (eng.flow_cancel(ref, b, cancelled)) ok(s); else nack(s);
        break;
      }
      case proto::MessageType::PUBLISH_MEASUREMENT: {
        CongestionDomainId d(r.u64());
        Measurement m;
        m.kind = static_cast<MeasurementKind>(r.u8());
        m.value = r.f64();
        m.provenance = static_cast<MeasurementProvenance>(r.u8());
        m.confidence = r.f64();
        // Live measurement publication requires an authoritative, registered
        // worker (fresh boot). A stale or unregistered session is rejected.
        if (!ctx_wid.is_valid()) { nack(s); break; }
        if (eng.publish_measurement(d, m, ctx_wid, ctx_boot)) ok(s); else nack(s);
        break;
      }
      case proto::MessageType::QUERY_CONGESTION: {
        CongestionDomainId d(r.u64());
        CongestionAssessment a = eng.recompute_domain(d);
        DomainSnapshot snap = eng.snapshot(d);
        Writer body;
        body.u8(static_cast<std::uint8_t>(a.state));
        body.u8(static_cast<std::uint8_t>(a.provenance));
        body.f64(a.score);
        body.u8(a.authoritative ? 1 : 0);
        body.u8(snap.offered_bps > 0 ? 1 : 0);
        body.f64(snap.offered_bps);
        body.f64(snap.serviced_bps);
        body.f64(snap.effective_bps);
        body.f64(snap.residual_bps);
        send_frame(s, proto::MessageType::CONGESTION_UPDATE, body.data());
        break;
      }
      case proto::MessageType::QUERY_BOTTLENECK: {
        std::uint64_t n = r.bounded_count(1024);
        std::vector<CongestionDomainId> path;
        for (std::uint64_t i = 0; i < n; ++i) path.push_back(CongestionDomainId(r.u64()));
        BottleneckResult br = eng.identify_bottleneck(path);
        Writer body;
        body.u8(br.domain.id.is_valid() ? 1 : 0);
        body.u64(br.domain.id.value());
        body.f64(br.deficit_bps);
        body.str(br.explanation);
        send_frame(s, proto::MessageType::BOTTLENECK, body.data());
        break;
      }
      case proto::MessageType::QUERY_RESIDUAL: {
        CongestionDomainId d(r.u64());
        double offered = r.f64();
        ResidualCapacityResult res = eng.query_residual_capacity(d, offered);
        Writer body;
        body.u8(res.has_capacity ? 1 : 0);
        body.f64(res.effective_bps);
        body.f64(res.residual_bps);
        send_frame(s, proto::MessageType::RESIDUAL, body.data());
        break;
      }
      case proto::MessageType::APPLY_SHAPING: {
        CongestionDomainId d(r.u64());
        BackpressureAction action;
        action.intent = static_cast<ActionIntent>(r.u8());
        action.generation = BackpressureGeneration(r.u64());
        action.expected_benefit_bps = r.f64();
        if (eng.apply_local_shaping(d, action)) ok(s); else nack(s);
        break;
      }
      case proto::MessageType::FENCE_WORKER: {
        WorkerId w(r.u64());
        WorkerBootId b(r.u64());
        if (eng.fence_worker(w, b)) ok(s); else nack(s);
        break;
      }
      case proto::MessageType::INVALIDATE_SOURCE: {
        SourceId s1(r.u64());
        SourceBootId s2(r.u64());
        if (eng.fence_source(s1, s2)) ok(s); else nack(s);
        break;
      }
      case proto::MessageType::ADVANCE_GENERATION: {
        EntityRef<Flow> ref;
        ref.id = FlowId(r.u64());
        ref.generation = FlowGeneration(r.u64());
        FlowGeneration next(r.u64());
        if (eng.advance_flow_generation(ref, next)) ok(s); else nack(s);
        break;
      }
      case proto::MessageType::REVALIDATE: {
        EntityRef<Flow> ref;
        ref.id = FlowId(r.u64());
        ref.generation = FlowGeneration(r.u64());
        WorkerBootId b(r.u64());
        if (eng.revalidate_flow(ref, b)) ok(s); else nack(s);
        break;
      }
      case proto::MessageType::SAVE: {
        std::string path = r.str();
        if (eng.save(path)) ok(s); else nack(s);
        break;
      }
      case proto::MessageType::SHUTDOWN: {
        ok(s);
        g_stop.store(true);
        net::close_socket(g_listener);
        close_other_socks(s);
        break;
      }
      default:
        nack(s);
        break;
    }
  } catch (const std::exception&) {
    nack(s);
  }
}

void session(SOCKET s, CongestionFabric& eng) {
  reg_sock(s);
  WorkerId wid;
  WorkerBootId boot;
  proto::Frame f;
  while (!g_stop.load() && recv_frame(s, f)) {
    handle(s, eng, f, wid, boot);
  }
  // Real worker death: the peer's socket dropped. Fence the worker so its
  // exclusive flow evidence becomes STALE and no completion is invented.
  if (wid.is_valid()) {
    eng.fence_worker(wid, boot);
    std::fprintf(stderr, "coordinator: worker %llu boot %llu disconnected; fenced\n",
                 (unsigned long long)wid.value(), (unsigned long long)boot.value());
  }
  unreg_sock(s);
  net::close_socket(s);
}

}  // namespace

int main(int argc, char** argv) {
  std::uint16_t port = 0;
  std::string state;
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--port" && i + 1 < argc) port = static_cast<std::uint16_t>(std::atoi(argv[++i]));
    else if (std::string(argv[i]) == "--state" && i + 1 < argc) state = argv[++i];
  }
  if (port == 0) { std::fprintf(stderr, "usage: cfcoordinator --port P [--state FILE]\n"); return 2; }
  if (!net::init()) { std::fprintf(stderr, "winsock init failed\n"); return 2; }

  CongestionFabric eng;
  if (!state.empty()) {
    if (eng.load(state)) {
      std::fprintf(stderr, "coordinator: loaded state from %s (epoch %llu)\n", state.c_str(),
                   (unsigned long long)eng.epoch().value());
    }
  }

  SOCKET listener = net::listen_socket(port);
  if (listener == INVALID_SOCKET) { std::fprintf(stderr, "listen failed on %u\n", port); net::cleanup(); return 2; }
  g_listener = listener;
  std::fprintf(stderr, "coordinator: listening on 127.0.0.1:%u\n", port);

  std::vector<std::thread> threads;
  while (!g_stop.load()) {
    SOCKET c = ::accept(listener, nullptr, nullptr);
    if (c == INVALID_SOCKET) {
      if (g_stop.load()) break;
      std::fprintf(stderr, "accept failed\n");
      continue;
    }
    std::fprintf(stderr, "coordinator: connection accepted\n");
    threads.emplace_back(session, c, std::ref(eng));
  }
  net::close_socket(listener);
  g_listener = INVALID_SOCKET;
  for (auto& t : threads) if (t.joinable()) t.join();

  if (!state.empty()) {
    eng.save(state);
  }
  std::fprintf(stderr, "coordinator: shutting down\n");
  net::cleanup();
  return 0;
}
