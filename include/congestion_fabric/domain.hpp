#pragma once
// Congestion Fabric -- explicit congestion domains and their queue/backlog +
// rate + shaping + propagation state. The engine is the single mutating
// authority; the domain is the indexed state holder.
#include "congestion_fabric/backpressure.hpp"
#include "congestion_fabric/capacity.hpp"
#include "congestion_fabric/checked.hpp"
#include "congestion_fabric/enums.hpp"
#include "congestion_fabric/episode.hpp"
#include "congestion_fabric/fairness.hpp"
#include "congestion_fabric/flow.hpp"
#include "congestion_fabric/measurement.hpp"
#include "congestion_fabric/propagation.hpp"
#include "congestion_fabric/shaping.hpp"
#include "congestion_fabric/types.hpp"
#include <array>
#include <cstdint>

namespace congfabric {

// Exact queue/backlog accounting per domain. All counts are optional-wrapped or
// checked; a queue never goes negative and stale completion can never drain a
// newer queue generation.
struct DomainQueue {
  checked::ByteCounter queued_bytes;
  std::uint64_t queued_ops{0};
  checked::ByteCounter in_flight_bytes;
  std::uint64_t in_flight_ops{0};
  checked::ByteCounter completed_bytes;
  checked::ByteCounter cancelled_bytes;
  checked::ByteCounter failed_bytes;
  QueueGeneration generation;

  bool valid() const noexcept { return queued_ops > 0 || queued_bytes.value() == 0; }
};

namespace detail {
// Per-traffic-class byte aggregate, indexed by the enum value.
template <class E, std::size_t N>
class ClassAggregate {
 public:
  void add(E k, std::uint64_t bytes) {
    auto& c = data_[static_cast<std::size_t>(k) % N];
    std::uint64_t o = 0;
    if (checked::add(c, bytes, o)) c = o;
  }
  void remove(E k, std::uint64_t bytes) {
    auto& c = data_[static_cast<std::size_t>(k) % N];
    std::uint64_t o = 0;
    if (checked::sub(c, bytes, o)) c = o;
  }
  std::uint64_t get(E k) const { return data_[static_cast<std::size_t>(k) % N]; }
 private:
  std::array<std::uint64_t, N> data_{};
};
}  // namespace detail

struct CongestionDomain {
  EntityRef<CongestionDomain> ref;
  DomainKind kind{DomainKind::UNKNOWN};
  CapacityProfile capacity;
  DomainQueue queue;
  MeasurementSet measurements;
  CongestionState state{CongestionState::UNKNOWN};
  MeasurementProvenance state_provenance{MeasurementProvenance::UNKNOWN};
  BackpressureGeneration backpressure_generation;
  RecoveryGeneration recovery_generation;
  FairnessConfig default_fairness;
  TokenBucket shaping;
  std::vector<EntityRef<Flow>> flows;
  detail::ClassAggregate<TrafficClass, static_cast<std::size_t>(TrafficClass::UNKNOWN) + 1>
      class_bytes;
  std::vector<DomainInfluence> outbound_influence;
  std::vector<CongestionDomainId> upstream_domains;
  std::vector<CongestionEpisode> episodes;
  CongestionEpisode current_episode;

  // Aggregate load rates with provenance (never presented as MEASURED unless
  // genuinely measured).
  double offered_bps{0.0};
  MeasurementProvenance offered_provenance{MeasurementProvenance::UNKNOWN};
  double admitted_bps{0.0};
  MeasurementProvenance admitted_provenance{MeasurementProvenance::UNKNOWN};
  double serviced_bps{0.0};
  MeasurementProvenance serviced_provenance{MeasurementProvenance::UNKNOWN};

  double utilisation{0.0};
  MeasurementProvenance utilisation_provenance{MeasurementProvenance::UNKNOWN};
  double effective_bps{0.0};
  MeasurementProvenance effective_provenance{MeasurementProvenance::UNKNOWN};
  double residual_bps{0.0};
  MeasurementProvenance residual_provenance{MeasurementProvenance::UNKNOWN};

  bool exists() const noexcept { return ref.id.is_valid(); }
  bool is_live() const noexcept {
    return congestion_is_live(state);
  }
};

}  // namespace congfabric
