#pragma once
// Congestion Fabric -- congestion episodes are first-class, generation-bound
// events. A later episode is never confused with an older one, and stale
// recovery evidence can never clear a current episode.
#include "congestion_fabric/backpressure.hpp"
#include "congestion_fabric/enums.hpp"
#include "congestion_fabric/types.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace congfabric {

struct CongestionEpisode {
  CongestionEventId event_id;
  CongestionEventGeneration generation;
  EntityRef<CongestionDomain> domain;
  CongestionState peak_state{CongestionState::UNKNOWN};
  std::uint64_t start_ms{0};
  std::uint64_t peak_ms{0};
  std::uint64_t recovery_start_ms{0};
  std::uint64_t end_ms{0};
  std::string trigger;
  double peak_backlog_bytes{0.0};
  double service_deficit_bytes{0.0};
  std::vector<FlowId> affected_flows;
  EntityRef<Resource> bottleneck;
  std::vector<BackpressureAction> actions;
  CongestionState outcome{CongestionState::UNKNOWN};
  bool open{true};

  bool valid() const noexcept {
    return event_id.is_valid() && generation.is_valid() &&
           domain.id.is_valid() && start_ms <= end_ms;
  }
};

}  // namespace congfabric
