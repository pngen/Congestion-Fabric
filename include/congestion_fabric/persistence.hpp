#pragma once
// Congestion Fabric -- versioned binary persistence primitives.
// Deterministic little-endian encoding with a CRC-32 payload check, bounded
// lengths and strict bounds-checked decoding. All corruption / truncation /
// trailing-garbage detection lives here and is exercised by tests.
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace congfabric::persist {

inline constexpr char kMagic[8] = {'C', 'O', 'N', 'G', 'F', 'A', 'B', '1'};
inline constexpr std::uint32_t kVersion = 1;
// Payload is strictly bounded so a hostile length cannot drive huge allocation.
inline constexpr std::uint64_t kMaxPayloadBytes = 1ULL << 30;  // 1 GiB
inline constexpr std::uint64_t kMaxStringBytes = 1ULL << 24;   // 16 MiB
inline constexpr std::uint64_t kMaxCollectionCount = 1ULL << 24;

class CorruptError : public std::runtime_error {
 public:
  explicit CorruptError(const std::string& m) : std::runtime_error(m) {}
};

// Standard CRC-32 (IEEE 802.3 / zlib polynomial, reflected), table-free.
inline std::uint32_t crc32(const std::uint8_t* data, std::size_t len,
                           std::uint32_t seed = 0) noexcept {
  std::uint32_t crc = ~seed;
  for (std::size_t i = 0; i < len; ++i) {
    crc ^= data[i];
    for (int b = 0; b < 8; ++b) {
      std::uint32_t mask = static_cast<std::uint32_t>(-static_cast<int32_t>(crc & 1u));
      crc = (crc >> 1) ^ (0xEDB88320u & mask);
    }
  }
  return ~crc;
}

class Writer {
 public:
  void u8(std::uint8_t v) { buf_.push_back(v); }
  void u16(std::uint16_t v) {
    u8(static_cast<std::uint8_t>(v & 0xFF));
    u8(static_cast<std::uint8_t>((v >> 8) & 0xFF));
  }
  void u32(std::uint32_t v) {
    for (int i = 0; i < 4; ++i) u8(static_cast<std::uint8_t>((v >> (8 * i)) & 0xFF));
  }
  void u64(std::uint64_t v) {
    for (int i = 0; i < 8; ++i) u8(static_cast<std::uint8_t>((v >> (8 * i)) & 0xFF));
  }
  void i64(std::int64_t v) { u64(static_cast<std::uint64_t>(v)); }
  void f64(double v) {
    std::uint64_t bits = 0;
    static_assert(sizeof(double) == sizeof(std::uint64_t), "double size");
    std::memcpy(&bits, &v, sizeof(v));
    u64(bits);
  }
  void raw(const std::uint8_t* d, std::size_t n) { buf_.insert(buf_.end(), d, d + n); }
  void bytes(const std::vector<std::uint8_t>& b) { raw(b.data(), b.size()); }
  void str(const std::string& s) {
    std::uint64_t n = static_cast<std::uint64_t>(s.size());
    u64(n);
    raw(reinterpret_cast<const std::uint8_t*>(s.data()), s.size());
  }
  std::size_t size() const { return buf_.size(); }
  const std::vector<std::uint8_t>& data() const { return buf_; }
  std::uint32_t crc() const { return crc32(buf_.data(), buf_.size()); }
  void clear() { buf_.clear(); }

 private:
  std::vector<std::uint8_t> buf_;
};

class Reader {
 public:
  Reader(const std::uint8_t* data, std::size_t len) : p_(data), len_(len) {}
  explicit Reader(const std::vector<std::uint8_t>& v)
      : p_(v.data()), len_(v.size()) {}

  std::size_t remaining() const { return len_ - off_; }
  bool empty() const { return off_ >= len_; }

  std::size_t offset() const { return off_; }
  void seek(std::size_t o) {
    if (o > len_) throw CorruptError("seek out of bounds");
    off_ = o;
  }

  std::uint8_t u8() {
    need(1);
    return p_[off_++];
  }
  std::uint16_t u16() {
    std::uint16_t v = static_cast<std::uint16_t>(u8());
    v |= static_cast<std::uint16_t>(u8()) << 8;
    return v;
  }
  std::uint32_t u32() {
    std::uint32_t v = 0;
    for (int i = 0; i < 4; ++i) v |= static_cast<std::uint32_t>(u8()) << (8 * i);
    return v;
  }
  std::uint64_t u64() {
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<std::uint64_t>(u8()) << (8 * i);
    return v;
  }
  std::int64_t i64() { return static_cast<std::int64_t>(u64()); }
  double f64() {
    std::uint64_t bits = u64();
    double v = 0.0;
    std::memcpy(&v, &bits, sizeof(v));
    return v;
  }
  void raw(std::uint8_t* d, std::size_t n) {
    need(n);
    std::memcpy(d, p_ + off_, n);
    off_ += n;
  }
  std::vector<std::uint8_t> bytes(std::size_t n) {
    need(n);
    std::vector<std::uint8_t> out(p_ + off_, p_ + off_ + n);
    off_ += n;
    return out;
  }
  std::string str() {
    std::uint64_t n = u64();
    if (n > kMaxStringBytes) throw CorruptError("string too long");
    need(static_cast<std::size_t>(n));
    std::string out(reinterpret_cast<const char*>(p_ + off_), n);
    off_ += static_cast<std::size_t>(n);
    return out;
  }
  // Reads a count that must not exceed the caller-supplied bound.
  std::uint64_t bounded_count(std::uint64_t bound) {
    std::uint64_t n = u64();
    if (n > bound) throw CorruptError("count exceeds bound");
    return n;
  }

 private:
  void need(std::size_t n) {
    if (n > len_ - off_) throw CorruptError("truncated data");
  }
  const std::uint8_t* p_;
  std::size_t len_;
  std::size_t off_{0};
};

}  // namespace congfabric::persist
