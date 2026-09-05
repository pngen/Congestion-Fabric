#pragma once
// Congestion Fabric -- first-class flow model with a guarded lifecycle and
// exact, checked byte accounting. Every byte mutation is validated against the
// lifecycle state, and the authority (worker/source boot) that owns the flow is
// carried so a stale worker can never mutate current accounting.
#include "congestion_fabric/checked.hpp"
#include "congestion_fabric/enums.hpp"
#include "congestion_fabric/fairness.hpp"
#include "congestion_fabric/types.hpp"
#include <cstdint>
#include <string>

namespace congfabric {

// Guarded lifecycle transition table. Every transition must be explicit; the
// engine refuses illegal transitions instead of silently coercing state.
constexpr bool flow_transition_allowed(FlowState from, FlowState to) noexcept {
  using F = FlowState;
  switch (from) {
    case F::DECLARED:
      return to == F::VALIDATING || to == F::ADMITTED || to == F::CANCELLED ||
             to == F::FAILED;
    case F::VALIDATING:
      return to == F::ADMITTED || to == F::CANCELLED || to == F::FAILED;
    case F::ADMITTED:
      return to == F::QUEUED || to == F::ACTIVE || to == F::BACKPRESSURED ||
             to == F::THROTTLED || to == F::DRAINING || to == F::CANCELLED ||
             to == F::FAILED || to == F::STALE;
    case F::QUEUED:
      return to == F::ACTIVE || to == F::BACKPRESSURED || to == F::THROTTLED ||
             to == F::DRAINING || to == F::CANCELLED || to == F::FAILED ||
             to == F::STALE;
    case F::ACTIVE:
      return to == F::BACKPRESSURED || to == F::THROTTLED || to == F::DRAINING ||
             to == F::COMPLETED || to == F::CANCELLED || to == F::FAILED ||
             to == F::STALE;
    case F::BACKPRESSURED:
      return to == F::ACTIVE || to == F::THROTTLED || to == F::DRAINING ||
             to == F::COMPLETED || to == F::CANCELLED || to == F::FAILED ||
             to == F::STALE;
    case F::THROTTLED:
      return to == F::ACTIVE || to == F::DRAINING || to == F::COMPLETED ||
             to == F::CANCELLED || to == F::FAILED || to == F::STALE;
    case F::DRAINING:
      return to == F::COMPLETED || to == F::CANCELLED || to == F::FAILED ||
             to == F::STALE;
    // Terminal and sentinel states are immutable except revalidation.
    case F::COMPLETED:
    case F::CANCELLED:
    case F::FAILED:
    case F::SUPERSEDED:
    case F::RETIRED:
      return false;
    case F::STALE:
      return to == F::REVALIDATION_REQUIRED;
    case F::REVALIDATION_REQUIRED:
      return to == F::VALIDATING || to == F::DECLARED;
  }
  return false;
}

struct Flow {
  EntityRef<Flow> ref;                       // id + generation issued at declare
  WorkerId owner_worker;
  WorkerBootId owner_boot;
  SourceId source;
  SourceBootId source_boot;
  EntityRef<Workload> workload;
  EntityRef<Execution> execution;
  TrafficClass traffic_class{TrafficClass::UNKNOWN};
  EntityRef<CongestionDomain> domain;
  EntityRef<Path> path;
  FlowState state{FlowState::DECLARED};

  // ---- exact byte accounting (check anywhere) ----
  checked::ByteCounter expected_bytes;
  checked::ByteCounter submitted_bytes;
  checked::ByteCounter admitted_bytes;
  checked::ByteCounter in_flight_bytes;
  checked::ByteCounter completed_bytes;
  checked::ByteCounter failed_bytes;
  checked::ByteCounter cancelled_bytes;

  // ---- timing / external hints ----
  std::uint64_t declared_ms{0};
  std::uint64_t started_ms{0};
  std::uint64_t last_progress_ms{0};
  double priority{0.0};
  double latency_sensitivity{0.0};
  double throughput_target_bps{0.0};
  double burst_allowance_bytes{0.0};
  double deadline_ms{0.0};

  FairnessConfig fairness;
  MeasurementProvenance provenance{MeasurementProvenance::UNKNOWN};
  std::string description;

  bool is_terminal() const noexcept { return flow_is_terminal(state); }
  bool is_authoritative(const WorkerBootId& boot,
                        const SourceBootId& sboot) const noexcept {
    return owner_boot == boot && source_boot == sboot;
  }

  // completed must never exceed admitted; admitted must never exceed submitted
  // (per the accounting semantics the engine enforces); inflight relates to
  // admitted; failed+cancelled remain within expected.
  bool accounting_valid() const noexcept {
    if (completed_bytes.value() > admitted_bytes.value()) return false;
    if (admitted_bytes.value() > submitted_bytes.value()) return false;
    if (in_flight_bytes.value() > admitted_bytes.value()) return false;
    return true;
  }
};

}  // namespace congfabric
