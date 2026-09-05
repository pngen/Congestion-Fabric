#pragma once
// Congestion Fabric -- checked arithmetic for byte/rate accounting.
// All byte/rate accounting must avoid silent wraparound. Every addition,
// subtraction and scaling goes through these helpers, which report overflow.
#include <cstdint>
#include <limits>
#include <optional>

namespace congfabric::checked {

inline bool add(std::uint64_t a, std::uint64_t b, std::uint64_t& out) noexcept {
  if (a > std::numeric_limits<std::uint64_t>::max() - b) return false;
  out = a + b;
  return true;
}

inline bool sub(std::uint64_t a, std::uint64_t b, std::uint64_t& out) noexcept {
  if (b > a) return false;
  out = a - b;
  return true;
}

inline bool mul(std::uint64_t a, std::uint64_t b, std::uint64_t& out) noexcept {
  if (a != 0 && b > std::numeric_limits<std::uint64_t>::max() / a) return false;
  out = a * b;
  return true;
}

// -- convenience: value-producing wrappers that return nullopt on overflow ----
inline std::uint64_t add_or(std::uint64_t a, std::uint64_t b,
                            std::uint64_t fallback = 0) noexcept {
  std::uint64_t o = 0;
  return add(a, b, o) ? o : fallback;
}

// -- a checked byte counter ----------------------------------------------------
// Holds a non-negative byte count; every mutation is checked and reports
// whether it succeeded. A failed mutation leaves the counter unchanged.
class ByteCounter {
 public:
  ByteCounter() = default;
  explicit ByteCounter(std::uint64_t v) noexcept : value_(v) {}
  std::uint64_t value() const noexcept { return value_; }
  explicit operator bool() const noexcept { return value_ != 0; }

  bool add(std::uint64_t n) noexcept {
    std::uint64_t o = 0;
    if (!checked::add(value_, n, o)) return false;
    value_ = o;
    return true;
  }
  bool sub(std::uint64_t n) noexcept {
    std::uint64_t o = 0;
    if (!checked::sub(value_, n, o)) return false;
    value_ = o;
    return true;
  }
  bool empty() const noexcept { return value_ == 0; }
  bool fits_in(std::uint64_t n) const noexcept { return value_ <= n; }

  friend bool operator==(const ByteCounter&, const ByteCounter&) = default;

 private:
  std::uint64_t value_{0};
};

}  // namespace congfabric::checked
