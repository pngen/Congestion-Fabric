#pragma once
// Congestion Fabric -- directional congestion propagation between domains.
// Represented as a bounded, directed influence graph. The engine refuses or
// bounds cycles so arbitrary recursive evaluation can never run forever.
#include "congestion_fabric/enums.hpp"
#include "congestion_fabric/types.hpp"
#include <vector>

namespace congfabric {

struct DomainInfluence {
  CongestionDomainId upstream;
  CongestionDomainId downstream;
  InfluenceKind kind{InfluenceKind::UNKNOWN};
  // Causal weight in [0,1] describing how strongly upstream pressure
  // propagates to downstream (or reverse backpressure).
  double weight{1.0};

  bool valid() const noexcept {
    return upstream.is_valid() && downstream.is_valid() &&
           upstream != downstream && kind != InfluenceKind::UNKNOWN &&
           weight >= 0.0 && weight <= 1.0;
  }
};

}  // namespace congfabric
