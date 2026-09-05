#pragma once
// Congestion Fabric -- strongly typed identities and generations.
// Each authority domain is a distinct C++ type so that a stale FlowGeneration,
// LinkGeneration, WorkerBootId, etc. can never be silently confused with a
// current one. No two semantically separate domains share an underlying tag.
#include <cstdint>
#include <compare>
#include <functional>
#include <ostream>
#include <string>
#include <limits>

namespace congfabric {

// ---------------------------------------------------------------------------
// Id<Tag>: a stable, non-zero identifier within one authority domain.
// ---------------------------------------------------------------------------
template <class Tag>
class Id {
 public:
  Id() = default;
  explicit Id(std::uint64_t value) noexcept : value_(value) {}
  Id(const Id&) = default;
  Id& operator=(const Id&) = default;

  std::uint64_t value() const noexcept { return value_; }
  bool is_valid() const noexcept { return value_ != 0; }
  explicit operator bool() const noexcept { return is_valid(); }
  std::string str() const { return std::to_string(value_); }

  friend auto operator<=>(const Id&, const Id&) = default;
  friend bool operator==(const Id&, const Id&) = default;

 private:
  std::uint64_t value_{0};
};

// ---------------------------------------------------------------------------
// Generation<Tag>: a monotonically non-decreasing freshness marker.
// value() == 0 denotes "no generation" / unknown / never issued.
// ---------------------------------------------------------------------------
template <class Tag>
class Generation {
 public:
  Generation() = default;
  explicit Generation(std::uint64_t value) noexcept : value_(value) {}
  Generation(const Generation&) = default;
  Generation& operator=(const Generation&) = default;

  std::uint64_t value() const noexcept { return value_; }
  bool is_valid() const noexcept { return value_ != 0; }
  explicit operator bool() const noexcept { return is_valid(); }
  std::string str() const { return std::to_string(value_); }

  // Bump once; saturates at max rather than wrapping to 0 (wraparound is a
  // checked-arithmetic violation for any authority domain).
  Generation& operator++() noexcept {
    if (value_ != std::numeric_limits<std::uint64_t>::max()) ++value_;
    return *this;
  }
  Generation& operator--() noexcept {
    if (value_ != 0) --value_;
    return *this;
  }
  Generation next() const noexcept {
    Generation g(*this);
    ++g;
    return g;
  }
  Generation previous() const noexcept {
    Generation g(*this);
    --g;
    return g;
  }

  friend auto operator<=>(const Generation&, const Generation&) = default;
  friend bool operator==(const Generation&, const Generation&) = default;

 private:
  std::uint64_t value_{0};
};

// ---------------------------------------------------------------------------
// EntityTraits<T>: maps an entity type to its strongly-typed Id / Generation.
// The primary template is intentionally undefined so a misuse (an entity
// without a registered authority domain) fails to compile instead of silently
// falling back to a generic integer pair.
// ---------------------------------------------------------------------------
template <class T>
struct EntityTraits;

// ---------------------------------------------------------------------------
// EntityRef<T>: id + generation bound together, typed per authority domain.
// ---------------------------------------------------------------------------
template <class T>
struct EntityRef {
  using IdT = typename EntityTraits<T>::Id;
  using GenT = typename EntityTraits<T>::Gen;
  IdT id;
  GenT generation;

  auto operator<=>(const EntityRef&) const = default;
  bool operator==(const EntityRef&) const = default;

  bool is_valid() const noexcept { return id.is_valid() && generation.is_valid(); }
};

// --- arithmetic on raw uint64 for generation math (checked) ----------------
namespace detail {
inline bool checked_add(std::uint64_t a, std::uint64_t b, std::uint64_t& out) noexcept {
  if (a > std::numeric_limits<std::uint64_t>::max() - b) return false;
  out = a + b;
  return true;
}
inline bool checked_sub(std::uint64_t a, std::uint64_t b, std::uint64_t& out) noexcept {
  if (b > a) return false;
  out = a - b;
  return true;
}
}  // namespace detail

}  // namespace congfabric

// --- hash support -----------------------------------------------------------
namespace std {
template <class Tag>
struct hash<congfabric::Id<Tag>> {
  std::size_t operator()(const congfabric::Id<Tag>& id) const noexcept {
    return std::hash<std::uint64_t>{}(id.value());
  }
};
template <class Tag>
struct hash<congfabric::Generation<Tag>> {
  std::size_t operator()(const congfabric::Generation<Tag>& g) const noexcept {
    return std::hash<std::uint64_t>{}(g.value());
  }
};
}  // namespace std
