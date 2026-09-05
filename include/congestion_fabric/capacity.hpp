#pragma once
// Congestion Fabric -- capacity / bandwidth model.
// Advertised bandwidth is not serviced bandwidth. A CapacityProfile carries the
// distinct bandwidth kinds and derives effective / residual / congested values
// with strict validity checks. A forecast/measurement is never silently treated
// as a different kind.
#include "congestion_fabric/enums.hpp"
#include "congestion_fabric/types.hpp"
#include <cmath>
#include <cstdint>
#include <optional>
#include <limits>

namespace congfabric {

// A single bandwidth rate (bytes/second) with its kind + provenance + freshness.
struct Rate {
  double bytes_per_second{0.0};
  BandwidthKind kind{BandwidthKind::UNKNOWN};
  MeasurementProvenance provenance{MeasurementProvenance::UNKNOWN};
  CapacityGeneration generation;

  Rate() = default;
  Rate(double bps, BandwidthKind k, MeasurementProvenance p, CapacityGeneration g = {})
      : bytes_per_second(bps), kind(k), provenance(p), generation(g) {}

  bool is_finite_nonneg() const noexcept {
    return std::isfinite(bytes_per_second) && bytes_per_second >= 0.0;
  }
  bool is_unknown() const noexcept { return kind == BandwidthKind::UNKNOWN; }
};

// Holds the distinct bandwidth kinds for one domain and derives effective and
// residual capacity using conservative (min / subtract) semantics.
class CapacityProfile {
 public:
  CapacityProfile() = default;

  CapacityProfile& set_theoretical(Rate r) { theoretical_ = r; return *this; }
  CapacityProfile& set_configured(Rate r) { configured_ = r; return *this; }
  CapacityProfile& set_measured_service(Rate r) { measured_service_ = r; return *this; }
  CapacityProfile& set_reserved(Rate r) { reserved_ = r; return *this; }
  CapacityProfile& set_generation(CapacityGeneration g) { generation_ = g; return *this; }

  const Rate& theoretical() const { return theoretical_; }
  const Rate& configured() const { return configured_; }
  const Rate& measured_service() const { return measured_service_; }
  const Rate& reserved() const { return reserved_; }
  CapacityGeneration generation() const { return generation_; }

  // Effective capacity = the smallest positive bound among the available
  // specification/measurement layers, reduced by any hard reservation.
  // Returns nullopt when no valid (finite, non-negative) bound is present.
  std::optional<double> effective() const {
    double bound = std::numeric_limits<double>::infinity();
    bool found = false;
    if (theoretical_.kind != BandwidthKind::UNKNOWN && theoretical_.is_finite_nonneg()) {
      bound = std::min(bound, theoretical_.bytes_per_second);
      found = true;
    }
    if (configured_.kind != BandwidthKind::UNKNOWN && configured_.is_finite_nonneg()) {
      bound = std::min(bound, configured_.bytes_per_second);
      found = true;
    }
    if (measured_service_.kind != BandwidthKind::UNKNOWN && measured_service_.is_finite_nonneg()) {
      bound = std::min(bound, measured_service_.bytes_per_second);
      found = true;
    }
    if (!found) return std::nullopt;
    if (reserved_.kind != BandwidthKind::UNKNOWN && reserved_.is_finite_nonneg()) {
      double r = reserved_.bytes_per_second;
      if (r > bound) return std::nullopt;  // reservation exceeds capacity: invalid
      bound -= r;
    }
    if (bound < 0.0 || !std::isfinite(bound)) return std::nullopt;
    return bound;
  }

  // Residual capacity below the effective capacity given the current admitted
  // + active offered load. Never negative.
  std::optional<double> residual(double offered_load_bps) const {
    auto eff = effective();
    if (!eff) return std::nullopt;
    if (offered_load_bps < 0.0 || !std::isfinite(offered_load_bps)) return std::nullopt;
    double r = *eff - offered_load_bps;
    if (r < 0.0) r = 0.0;
    return r;
  }

  // Reservation cannot exceed the smallest available bound.
  bool reservation_is_consistent() const {
    auto eff = effective_before_reservation();
    if (!eff) return reserved_.kind == BandwidthKind::UNKNOWN;
    if (reserved_.kind == BandwidthKind::UNKNOWN) return true;
    return reserved_.is_finite_nonneg() && reserved_.bytes_per_second <= *eff;
  }

  // Whether the profile is internally valid (no NaN/Inf/negative rates).
  bool valid() const {
    auto check = [](const Rate& r) {
      if (r.kind == BandwidthKind::UNKNOWN) return true;
      return r.is_finite_nonneg();
    };
    return check(theoretical_) && check(configured_) && check(measured_service_) &&
           check(reserved_) && reservation_is_consistent();
  }

 private:
  std::optional<double> effective_before_reservation() const {
    double bound = std::numeric_limits<double>::infinity();
    bool found = false;
    for (const Rate* r : {&theoretical_, &configured_, &measured_service_}) {
      if (r->kind != BandwidthKind::UNKNOWN && r->is_finite_nonneg()) {
        bound = std::min(bound, r->bytes_per_second);
        found = true;
      }
    }
    if (!found) return std::nullopt;
    return bound;
  }

  Rate theoretical_;
  Rate configured_;
  Rate measured_service_;
  Rate reserved_;
  CapacityGeneration generation_;
};

}  // namespace congfabric
