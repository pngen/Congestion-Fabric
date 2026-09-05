#pragma once
// Congestion Fabric -- bounded backpressure / action intents.
// Congestion Fabric may generate an intent. It never directly re-routes,
// re-schedules or preempts; those belong to adjacent runtimes.
#include "congestion_fabric/enums.hpp"
#include "congestion_fabric/types.hpp"
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

namespace congfabric {

// A typed, bounded action intent targeting a flow/domain.
struct BackpressureAction {
  ActionIntent intent{ActionIntent::NO_ACTION};
  EntityRef<Flow> target_flow;
  EntityRef<CongestionDomain> target_domain;
  std::string cause;
  double expected_benefit_bps{0.0};
  double estimated_cost_bps{0.0};
  double cost_ms{0.0};
  CongestionPolicyGeneration policy_generation;
  BackpressureGeneration generation;
  MeasurementProvenance provenance{MeasurementProvenance::UNKNOWN};
  CoordinatorEpoch epoch;
  double validity_window_ms{0.0};
  std::vector<std::string> preconditions;

  bool valid() const noexcept {
    bool ok = action_intent_valid(static_cast<std::uint8_t>(intent));
    ok = ok && std::isfinite(expected_benefit_bps) && expected_benefit_bps >= 0.0;
    ok = ok && std::isfinite(estimated_cost_bps) && estimated_cost_bps >= 0.0;
    ok = ok && std::isfinite(cost_ms) && cost_ms >= 0.0;
    ok = ok && std::isfinite(validity_window_ms) && validity_window_ms >= 0.0;
    return ok;
  }
  bool is_current(const BackpressureGeneration& current) const noexcept {
    return !current.is_valid() || generation.is_valid() &&
           current.value() >= generation.value();
  }
};

constexpr bool intent_is_restrictive(ActionIntent i) noexcept {
  switch (i) {
    case ActionIntent::ADMIT_LIMITED:
    case ActionIntent::THROTTLE:
    case ActionIntent::DEFER:
    case ActionIntent::PAUSE_NEW_TRAFFIC:
    case ActionIntent::DRAIN:
    case ActionIntent::REDUCE_BURST:
      return true;
    default:
      return false;
  }
}

}  // namespace congfabric
