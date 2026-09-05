#pragma once
// Congestion Fabric -- reference framed protocol.
// Compact, versioned, bounded, checksummed frames. Every frame carries enough
// identity / generation / worker-boot authority for the receiver to reject
// stale or replayed traffic. Encoding is deterministic.
#include "congestion_fabric/persistence.hpp"
// Windows headers (wingdi.h / winnt.h) define generic macros (ERROR, DELETE)
// that would otherwise break the scoped enum enumerators below. Undefine them
// locally so the protocol enum is portable.
#ifdef ERROR
#undef ERROR
#endif
#ifdef DELETE
#undef DELETE
#endif
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace congfabric::proto {

inline constexpr std::uint8_t kMagic[4] = {'C', 'F', 'P', '1'};
inline constexpr std::uint8_t kVersion = 1;
inline constexpr std::uint64_t kMaxFrameBody = 1ULL << 20;  // 1 MiB bounded
// Header is magic(4) + version(1) + type(1) + body_len(4) = 10 bytes.
inline constexpr std::size_t kHeaderBytes = 10;
inline constexpr std::size_t kCrcBytes = 4;

enum class MessageType : std::uint8_t {
  HELLO = 1,
  REGISTER = 2,
  REGISTER_ACK = 3,
  CREATE_DOMAIN = 4,
  DOMAIN_CREATED = 5,
  DECLARE_FLOW = 6,
  FLOW_DECLARED = 7,
  ADMIT = 8,
  QUEUE = 9,
  START = 10,
  PROGRESS = 11,
  COMPLETE = 12,
  CANCEL = 13,
  PUBLISH_MEASUREMENT = 14,
  QUERY_CONGESTION = 15,
  CONGESTION_UPDATE = 16,
  QUERY_BOTTLENECK = 17,
  BOTTLENECK = 18,
  QUERY_RESIDUAL = 19,
  RESIDUAL = 20,
  APPLY_SHAPING = 21,
  BACKPRESSURE = 22,
  FENCE_WORKER = 23,
  INVALIDATE_SOURCE = 24,
  ADVANCE_GENERATION = 25,
  REVALIDATE = 26,
  SAVE = 27,
  SHUTDOWN = 28,
  NACK = 29,
  OK = 30,
  ERROR = 31
};

// A decoded frame: message type + opaque body bytes.
struct Frame {
  MessageType type;
  std::vector<std::uint8_t> body;
};

// CRC is computed over the type byte concatenated with the body.
inline std::uint32_t body_crc(std::uint8_t type, const std::uint8_t* body,
                              std::size_t n) noexcept {
  persist::Writer tmp;
  tmp.u8(type);
  tmp.raw(body, n);
  return tmp.crc();
}
inline std::uint32_t body_crc(std::uint8_t type,
                              const std::vector<std::uint8_t>& body) noexcept {
  return body_crc(type, body.data(), body.size());
}

// Encode a frame into a contiguous buffer.
inline std::vector<std::uint8_t> encode_frame(const Frame& f) {
  persist::Writer w;
  w.raw(kMagic, 4);
  w.u8(kVersion);
  w.u8(static_cast<std::uint8_t>(f.type));
  w.u32(static_cast<std::uint32_t>(f.body.size()));
  if (!f.body.empty()) w.raw(f.body.data(), f.body.size());
  w.u32(body_crc(static_cast<std::uint8_t>(f.type), f.body));
  return w.data();
}

// Decode exactly one frame located at the start of the buffer. The whole buffer
// must be exactly one frame (trailing garbage is rejected). Used for
// in-memory / test paths; the socket path reads header + body + crc via
// read_exact and then validates the CRC separately.
inline Frame decode_frame(const std::uint8_t* data, std::size_t len) {
  persist::Reader r(data, len);
  std::uint8_t magic[4];
  r.raw(magic, 4);
  if (std::memcmp(magic, kMagic, 4) != 0)
    throw persist::CorruptError("bad protocol magic");
  std::uint8_t ver = r.u8();
  if (ver != kVersion) throw persist::CorruptError("unsupported protocol version");
  std::uint8_t type = r.u8();
  std::uint32_t body_len = r.u32();
  if (body_len > kMaxFrameBody) throw persist::CorruptError("frame body too large");
  if (r.remaining() != static_cast<std::size_t>(body_len) + kCrcBytes)
    throw persist::CorruptError("bad frame length");
  const std::uint8_t* body_start = data + kHeaderBytes;
  std::uint32_t expect = body_crc(type, body_start, body_len);
  std::uint32_t stored = 0;
  const std::uint8_t* crc_ptr = body_start + body_len;
  for (int i = 0; i < 4; ++i)
    stored |= static_cast<std::uint32_t>(crc_ptr[i]) << (8 * i);
  if (stored != expect) throw persist::CorruptError("frame checksum mismatch");
  Frame f;
  f.type = static_cast<MessageType>(type);
  f.body.assign(body_start, body_start + body_len);
  return f;
}

// Validate the CRC of a frame assembled from parts (used by the socket path).
inline bool validate_crc(std::uint8_t type, const std::uint8_t* body,
                         std::size_t body_len, std::uint32_t stored) noexcept {
  return body_crc(type, body, body_len) == stored;
}

}  // namespace congfabric::proto
