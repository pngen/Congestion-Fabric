#pragma once
// Congestion Fabric -- local congestion-domain fairness.
// Local resource-domain governance only. This is never a global scheduler.
#include "congestion_fabric/enums.hpp"
#include "congestion_fabric/types.hpp"
#include <cmath>
#include <cstdint>
#include <vector>
#include <string>

namespace congfabric {

// Per-flow/group fairness configuration (weights, guarantee, cap, protection).
struct FairnessConfig {
  FairnessModel model{FairnessModel::EQUAL_SHARE};
  double weight{1.0};
  double minimum_guarantee_bps{0.0};      // minimum share
  double cap_bps{0.0};                    // maximum share; 0 == uncapped
  bool latency_critical_protected{false};
  CongestionPolicyGeneration policy_generation;

  bool valid() const noexcept {
    return std::isfinite(weight) && weight > 0.0 &&
           std::isfinite(minimum_guarantee_bps) && minimum_guarantee_bps >= 0.0 &&
           std::isfinite(cap_bps) && cap_bps >= 0.0;
  }
  // A stale policy generation must not drive current throttling decisions.
  bool is_current(const CongestionPolicyGeneration& current) const noexcept {
    return !policy_generation.is_valid() || current.is_valid() &&
           current.value() >= policy_generation.value();
  }
};

// Observed fairness outcome for a participant.
struct FairnessShare {
  FlowId flow;
  FlowGeneration flow_generation;
  FairnessConfig config;
  double target_share_bps{0.0};
  double observed_share_bps{0.0};
  double fairness_deficit_bps{0.0};  // target - observed, clamped >= 0
  bool starved{false};
  MeasurementProvenance provenance{MeasurementProvenance::UNKNOWN};

  bool valid() const noexcept {
    return std::isfinite(target_share_bps) && target_share_bps >= 0.0 &&
           std::isfinite(observed_share_bps) && observed_share_bps >= 0.0 &&
           std::isfinite(fairness_deficit_bps) && fairness_deficit_bps >= 0.0;
  }
};

}  // namespace congfabric
