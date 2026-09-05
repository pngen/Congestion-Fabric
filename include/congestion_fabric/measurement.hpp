#pragma once
// Congestion Fabric -- measurement model.
// Every observation carries provenance. DERIVED / SYNTHETIC / ESTIMATED evidence
// must never be presented as a physical MEASURED value.
#include "congestion_fabric/enums.hpp"
#include "congestion_fabric/types.hpp"
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>

namespace congfabric {

// A single typed observation.
struct Measurement {
  MeasurementKind kind{MeasurementKind::UNKNOWN};
  double value{0.0};
  MeasurementProvenance provenance{MeasurementProvenance::UNKNOWN};
  MeasurementGeneration generation;
  std::uint64_t timestamp_ms{0};
  double confidence{1.0};

  bool is_physical() const noexcept {
    return provenance == MeasurementProvenance::MEASURED;
  }
  bool valid() const noexcept {
    return std::isfinite(value) && !std::isinf(value) && confidence >= 0.0 &&
           confidence <= 1.0;
  }
};

// A bounded, indexed set of measurements keyed by kind (latest wins per kind).
class MeasurementSet {
 public:
  void put(Measurement m) { if (!m.valid()) return; latest_by_kind_[static_cast<size_t>(m.kind)] = m; }
  const Measurement* get(MeasurementKind k) const {
    auto& m = latest_by_kind_[static_cast<size_t>(k)];
    return m.kind == k ? &m : nullptr;
  }
  void clear() { latest_by_kind_ = {}; }
  std::size_t size() const { return latest_by_kind_.size(); }

 private:
  std::array<Measurement, static_cast<size_t>(MeasurementKind::UNKNOWN) + 1> latest_by_kind_{};
};

}  // namespace congfabric
