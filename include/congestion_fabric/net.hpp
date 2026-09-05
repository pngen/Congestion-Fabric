#pragma once
// Congestion Fabric -- minimal Winsock TCP helpers for the reference
// multiprocess deployment. Blocking sockets; frames are read/written with
// exact-length I/O (partial reads/writes handled).
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace congfabric::net {

inline bool init() {
  WSADATA d;
  return WSAStartup(MAKEWORD(2, 2), &d) == 0;
}
inline void cleanup() { WSACleanup(); }

inline SOCKET listen_socket(std::uint16_t port) {
  SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (s == INVALID_SOCKET) return INVALID_SOCKET;
  int yes = 1;
  setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&yes),
             sizeof(yes));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(port);
  if (::bind(s, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) ==
      SOCKET_ERROR) {
    ::closesocket(s);
    return INVALID_SOCKET;
  }
  if (::listen(s, 16) == SOCKET_ERROR) {
    ::closesocket(s);
    return INVALID_SOCKET;
  }
  return s;
}

inline SOCKET connect_socket(std::uint16_t port) {
  SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (s == INVALID_SOCKET) return INVALID_SOCKET;
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(port);
  if (::connect(s, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) ==
      SOCKET_ERROR) {
    ::closesocket(s);
    return INVALID_SOCKET;
  }
  return s;
}

// Exact-length read, robust to partial reads. Returns false on EOF/error.
inline bool read_exact(SOCKET s, char* buf, std::size_t n) {
  std::size_t got = 0;
  while (got < n) {
    int r = ::recv(s, buf + got, static_cast<int>(n - got), 0);
    if (r == 0) return false;      // peer closed / connection reset
    if (r == SOCKET_ERROR) return false;
    got += static_cast<std::size_t>(r);
  }
  return true;
}

// Exact-length write, robust to partial writes.
inline bool write_all(SOCKET s, const char* buf, std::size_t n) {
  std::size_t sent = 0;
  while (sent < n) {
    int r = ::send(s, buf + sent, static_cast<int>(n - sent), 0);
    if (r == SOCKET_ERROR) return false;
    sent += static_cast<std::size_t>(r);
  }
  return true;
}

inline void close_socket(SOCKET s) { ::closesocket(s); }

}  // namespace congfabric::net
