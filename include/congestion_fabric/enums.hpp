#pragma once
// Congestion Fabric -- taxonomy enums.
#include <cstdint>
#include <string>

namespace congfabric {

// --- congestion domain kinds ------------------------------------------------
enum class DomainKind : std::uint8_t {
  PCIE_LINK,
  PCIE_ROOT_COMPLEX,
  GPU_COPY_ENGINE,
  GPU_HOST_TRANSFER,
  GPU_PEER_PATH,
  NVLINK_CLASS_PATH,
  NIC_TX,
  NIC_RX,
  NETWORK_PATH,
  STORAGE_READ,
  STORAGE_WRITE,
  STORAGE_QUEUE,
  HOST_MEMORY_BANDWIDTH,
  PINNED_HOST_MEMORY_PATH,
  NUMA_MEMORY_PATH,
  COLLECTIVE_PATH,
  COLLECTIVE_GROUP,
  SHARED_TRANSFER_POOL,
  COMPOSITE_PATH,
  UNKNOWN
};

// --- traffic classes ---------------------------------------------------------
enum class TrafficClass : std::uint8_t {
  LATENCY_CRITICAL,
  THROUGHPUT,
  BULK_TRANSFER,
  CHECKPOINT,
  RECOVERY,
  MODEL_LOAD,
  KV_STATE,
  COLLECTIVE,
  REPLICATION,
  BACKGROUND,
  CONTROL,
  UNKNOWN
};

// --- measurement kinds ----------------------------------------------------------
enum class MeasurementKind : std::uint8_t {
  OFFERED_BYTES_PER_SEC,
  ADMITTED_BYTES_PER_SEC,
  COMPLETED_BYTES_PER_SEC,
  QUEUE_DEPTH_OPS,
  QUEUED_BYTES,
  IN_FLIGHT_BYTES,
  SERVICE_LATENCY,
  QUEUEING_LATENCY,
  COMPLETION_LATENCY,
  RETRY_VOLUME,
  ACTIVE_FLOW_COUNT,
  BURST_RATE,
  UTILIZATION,
  EFFECTIVE_BANDWIDTH,
  SERVICE_DEFICIT,
  BACKLOG_GROWTH_RATE,
  FAIRNESS,
  CONGESTION_DURATION,
  RECOVERY_DURATION,
  UNKNOWN
};

// --- flow lifecycle states ----------------------------------------------------
enum class FlowState : std::uint8_t {
  DECLARED,
  VALIDATING,
  ADMITTED,
  QUEUED,
  ACTIVE,
  BACKPRESSURED,
  THROTTLED,
  DRAINING,
  COMPLETED,
  CANCELLED,
  FAILED,
  STALE,
  SUPERSEDED,
  REVALIDATION_REQUIRED,
  RETIRED
};

// --- congestion states ---------------------------------------------------------
enum class CongestionState : std::uint8_t {
  UNKNOWN,
  IDLE,
  HEALTHY,
  BUSY,
  APPROACHING_SATURATION,
  SATURATED,
  QUEUEING,
  CONGESTED,
  SEVERELY_CONGESTED,
  BACKPRESSURED,
  RECOVERING,
  STALE,
  REVALIDATION_REQUIRED
};

// --- measurement provenance -----------------------------------------------------
enum class MeasurementProvenance : std::uint8_t {
  MEASURED,
  REPORTED,
  DERIVED,
  ESTIMATED,
  SYNTHETIC,
  RECONSTRUCTED,
  UNKNOWN
};

// --- bounded action intent -------------------------------------------------------
enum class ActionIntent : std::uint8_t {
  ALLOW,
  ADMIT_LIMITED,
  THROTTLE,
  DEFER,
  PAUSE_NEW_TRAFFIC,
  DRAIN,
  REDUCE_BURST,
  REQUEST_REROUTE,
  REQUEST_REPLAN,
  REQUEST_RESCHEDULE,
  REQUEST_PREEMPTION,
  REVALIDATE,
  NO_ACTION,
  UNKNOWN
};

// --- bandwidth kinds --------------------------------------------------------------
enum class BandwidthKind : std::uint8_t {
  THEORETICAL,
  CONFIGURED,
  MEASURED_SERVICE,
  EFFECTIVE_WORKLOAD,
  RESIDUAL,
  RESERVED,
  CONGESTED,
  UNKNOWN
};

// --- local fairness models ---------------------------------------------------------
enum class FairnessModel : std::uint8_t {
  EQUAL_SHARE,
  WEIGHTED_SHARE,
  MINIMUM_GUARANTEED_SHARE,
  CAPPED_SHARE,
  PROTECTED_LATENCY_CRITICAL_SHARE,
  CALLER_WEIGHTS,
  UNKNOWN
};

// --- propagation influence sign -----------------------------------------------------
enum class InfluenceKind : std::uint8_t {
  FORWARD,   // upstream -> downstream pressure
  REVERSE,   // downstream backpressure to upstream
  UNKNOWN
};

// ---- enum mapping / validation helpers for persistence -------------------------
constexpr std::uint8_t to_u8(DomainKind v) noexcept { return static_cast<std::uint8_t>(v); }
constexpr std::uint8_t to_u8(TrafficClass v) noexcept { return static_cast<std::uint8_t>(v); }
constexpr std::uint8_t to_u8(FlowState v) noexcept { return static_cast<std::uint8_t>(v); }
constexpr std::uint8_t to_u8(CongestionState v) noexcept { return static_cast<std::uint8_t>(v); }
constexpr std::uint8_t to_u8(MeasurementProvenance v) noexcept { return static_cast<std::uint8_t>(v); }
constexpr std::uint8_t to_u8(MeasurementKind v) noexcept { return static_cast<std::uint8_t>(v); }
constexpr std::uint8_t to_u8(ActionIntent v) noexcept { return static_cast<std::uint8_t>(v); }
constexpr std::uint8_t to_u8(BandwidthKind v) noexcept { return static_cast<std::uint8_t>(v); }
constexpr std::uint8_t to_u8(FairnessModel v) noexcept { return static_cast<std::uint8_t>(v); }
constexpr std::uint8_t to_u8(InfluenceKind v) noexcept { return static_cast<std::uint8_t>(v); }

template <class E>
constexpr std::uint8_t enum_max_cnt() noexcept {
  return static_cast<std::uint8_t>(E::UNKNOWN) + 1;
}

template <class E>
constexpr bool enum_valid(std::uint8_t u) noexcept {
  return u <= static_cast<std::uint8_t>(E::UNKNOWN);
}

constexpr bool domain_kind_valid(std::uint8_t u) noexcept { return enum_valid<DomainKind>(u); }
constexpr bool traffic_class_valid(std::uint8_t u) noexcept { return enum_valid<TrafficClass>(u); }
// FlowState and CongestionState have no UNKNOWN sentinel, so their maximum
// valid enumerator is checked directly.
constexpr bool flow_state_valid(std::uint8_t u) noexcept {
  return u <= static_cast<std::uint8_t>(FlowState::RETIRED);
}
constexpr bool congestion_state_valid(std::uint8_t u) noexcept {
  return u <= static_cast<std::uint8_t>(CongestionState::REVALIDATION_REQUIRED);
}
constexpr bool provenance_valid(std::uint8_t u) noexcept { return enum_valid<MeasurementProvenance>(u); }
constexpr bool measurement_kind_valid(std::uint8_t u) noexcept { return u <= static_cast<std::uint8_t>(MeasurementKind::UNKNOWN); }
constexpr bool action_intent_valid(std::uint8_t u) noexcept { return enum_valid<ActionIntent>(u); }
constexpr bool bandwidth_kind_valid(std::uint8_t u) noexcept { return enum_valid<BandwidthKind>(u); }
constexpr bool fairness_model_valid(std::uint8_t u) noexcept { return enum_valid<FairnessModel>(u); }
constexpr bool influence_kind_valid(std::uint8_t u) noexcept { return enum_valid<InfluenceKind>(u); }

// Flow lifecycle terminal / transitional predicates ---------------------------
constexpr bool flow_is_terminal(FlowState s) noexcept {
  return s == FlowState::COMPLETED || s == FlowState::CANCELLED ||
         s == FlowState::FAILED || s == FlowState::RETIRED;
}
constexpr bool flow_is_stale_or_superseded(FlowState s) noexcept {
  return s == FlowState::STALE || s == FlowState::SUPERSEDED || s == FlowState::RETIRED;
}
// States from which a flow may still account bytes / be progressed.
constexpr bool flow_can_progress(FlowState s) noexcept {
  return s == FlowState::QUEUED || s == FlowState::ACTIVE ||
         s == FlowState::BACKPRESSURED || s == FlowState::THROTTLED ||
         s == FlowState::DRAINING;
}

// Congestion states that represent "live" (non-stale) evidence is required.
constexpr bool congestion_is_live(CongestionState s) noexcept {
  return s != CongestionState::STALE && s != CongestionState::REVALIDATION_REQUIRED &&
         s != CongestionState::UNKNOWN;
}

}  // namespace congfabric
