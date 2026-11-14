// Congestion Fabric coordinator regression: a session whose REGISTER was rejected
// is not bound to the worker, so its disconnect does not fence the live
// incarnation. Spawns the real cfcoordinator process and talks framed TCP over
// loopback. The rejected session half-closes and reads to EOF before the live
// session publishes: the coordinator fences (or not) before closing its end.
#include "congestion_fabric/net.hpp"
#include "congestion_fabric/enums.hpp"
#include "congestion_fabric/persistence.hpp"
#include "congestion_fabric/protocol.hpp"
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#ifndef CF_COORDINATOR_EXE
#define CF_COORDINATOR_EXE "cfcoordinator"
#endif

using namespace congfabric;
using congfabric::persist::Reader;
using congfabric::persist::Writer;

namespace {

int fails = 0;
void chk(bool c, const char* m) {
  if (!c) { std::printf("FAIL: %s\n", m); ++fails; }
}

bool send_frame(SOCKET s, proto::MessageType t, const std::vector<std::uint8_t>& body) {
  proto::Frame f;
  f.type = t;
  f.body = body;
  const std::vector<std::uint8_t> bytes = proto::encode_frame(f);
  return net::write_all(s, reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

bool recv_frame(SOCKET s, proto::Frame& out) {
  char hdr[proto::kHeaderBytes];
  if (!net::read_exact(s, hdr, proto::kHeaderBytes)) return false;
  if (std::memcmp(hdr, proto::kMagic, 4) != 0) return false;
  const std::uint8_t type = static_cast<std::uint8_t>(hdr[5]);
  std::uint32_t body_len = 0;
  for (int i = 0; i < 4; ++i) body_len |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(hdr[6 + i])) << (8 * i);
  if (body_len > proto::kMaxFrameBody) return false;
  std::vector<std::uint8_t> body(body_len);
  if (body_len && !net::read_exact(s, reinterpret_cast<char*>(body.data()), body_len)) return false;
  char cb[4];
  if (!net::read_exact(s, cb, 4)) return false;
  out.type = static_cast<proto::MessageType>(type);
  out.body = std::move(body);
  return true;
}

bool exchange(SOCKET s, proto::MessageType t, const std::vector<std::uint8_t>& body, proto::Frame& reply) {
  return send_frame(s, t, body) && recv_frame(s, reply);
}

std::vector<std::uint8_t> register_body(std::uint64_t worker, std::uint64_t boot) {
  Writer w;
  w.u64(worker); w.u64(boot); w.u64(worker); w.u64(boot);   // worker, boot, source, source boot
  return w.data();
}

std::vector<std::uint8_t> offered_body(std::uint64_t domain, double bps) {
  Writer w;
  w.u64(domain);
  w.u8(static_cast<std::uint8_t>(MeasurementKind::OFFERED_BYTES_PER_SEC));
  w.f64(bps);
  w.u8(static_cast<std::uint8_t>(MeasurementProvenance::REPORTED));
  w.f64(1.0);
  return w.data();
}

std::uint16_t free_port() {
  SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  a.sin_port = 0;
  int len = static_cast<int>(sizeof(a));
  ::bind(s, reinterpret_cast<sockaddr*>(&a), len);
  ::getsockname(s, reinterpret_cast<sockaddr*>(&a), &len);
  const std::uint16_t port = ntohs(a.sin_port);
  ::closesocket(s);
  return port;
}

// Connects once the coordinator listens; gives up only if the process exits.
SOCKET connect_when_ready(HANDLE proc, std::uint16_t port) {
  for (;;) {
    SOCKET s = net::connect_socket(port);
    if (s != INVALID_SOCKET) return s;
    if (WaitForSingleObject(proc, 0) == WAIT_OBJECT_0) return INVALID_SOCKET;
    Sleep(10);
  }
}

}  // namespace

int main() {
  if (!net::init()) { std::printf("FAIL: winsock init\n"); return 1; }
  const std::uint16_t port = free_port();
  std::string cmd = std::string("\"") + CF_COORDINATOR_EXE + "\" --port " + std::to_string(port);
  std::vector<char> cmdline(cmd.begin(), cmd.end());
  cmdline.push_back('\0');
  STARTUPINFOA si{};
  si.cb = sizeof(si);
  PROCESS_INFORMATION pi{};
  if (!CreateProcessA(nullptr, cmdline.data(), nullptr, nullptr, FALSE, CREATE_NO_WINDOW, nullptr, nullptr, &si, &pi)) {
    std::printf("FAIL: cannot start %s\n", CF_COORDINATOR_EXE);
    return 1;
  }
  CloseHandle(pi.hThread);
  bool shut_down = false;
  SOCKET live = connect_when_ready(pi.hProcess, port);
  chk(live != INVALID_SOCKET, "connect live session");
  if (live != INVALID_SOCKET) {
    proto::Frame r;
    chk(exchange(live, proto::MessageType::REGISTER, register_body(1, 5), r) &&
            r.type == proto::MessageType::REGISTER_ACK && !r.body.empty() && r.body[0] == 1,
        "live incarnation registers (worker 1, boot 5)");
    Writer dom;
    dom.u8(static_cast<std::uint8_t>(DomainKind::NETWORK_PATH));
    dom.u8(static_cast<std::uint8_t>(BandwidthKind::THEORETICAL)); dom.f64(100e6);
    dom.u8(static_cast<std::uint8_t>(BandwidthKind::CONFIGURED)); dom.f64(100e6);
    std::uint64_t domain = 0;
    if (exchange(live, proto::MessageType::CREATE_DOMAIN, dom.data(), r) && r.type == proto::MessageType::DOMAIN_CREATED) {
      Reader rd(r.body);
      chk(rd.u8() == 1, "domain created");
      domain = rd.u64();
    } else {
      chk(false, "create domain");
    }
    chk(exchange(live, proto::MessageType::PUBLISH_MEASUREMENT, offered_body(domain, 50e6), r) &&
            r.type == proto::MessageType::OK,
        "live incarnation publishes");

    // A second session replays the live boot: REGISTER is rejected, then the
    // session closes (half-close barrier: read to EOF).
    SOCKET replay = net::connect_socket(port);
    chk(replay != INVALID_SOCKET, "connect replay session");
    if (replay != INVALID_SOCKET) {
      chk(exchange(replay, proto::MessageType::REGISTER, register_body(1, 5), r) &&
              r.type == proto::MessageType::REGISTER_ACK && !r.body.empty() && r.body[0] == 0,
          "replayed REGISTER rejected");
      ::shutdown(replay, SD_SEND);
      char buf[64];
      while (::recv(replay, buf, static_cast<int>(sizeof(buf)), 0) > 0) {}
      ::closesocket(replay);
    }

    // The live incarnation is still authoritative.
    chk(exchange(live, proto::MessageType::PUBLISH_MEASUREMENT, offered_body(domain, 60e6), r) &&
            r.type == proto::MessageType::OK,
        "live incarnation still publishes after the rejected session closed");

    shut_down = exchange(live, proto::MessageType::SHUTDOWN, {}, r) && r.type == proto::MessageType::OK;
    chk(shut_down, "coordinator acknowledges SHUTDOWN");
    ::closesocket(live);
  }
  if (!shut_down) TerminateProcess(pi.hProcess, 1);
  WaitForSingleObject(pi.hProcess, INFINITE);
  DWORD code = 1;
  GetExitCodeProcess(pi.hProcess, &code);
  CloseHandle(pi.hProcess);
  if (shut_down) chk(code == 0, "coordinator exits cleanly");
  net::cleanup();
  std::printf("cf_coordinator_session: %s (%d failures)\n", fails == 0 ? "OK" : "FAIL", fails);
  return fails == 0 ? 0 : 1;
}
