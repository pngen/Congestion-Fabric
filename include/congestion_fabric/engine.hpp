#pragma once
// Congestion Fabric -- the central engine.
// Holds the authoritative generation/epoch state, indexed domains and flows,
// and the congestion assessment / bottleneck / fairness / backpressure /
// shaping operations. All external mutation goes through this class so
// invariants (authority freshness, checked accounting, exact queue closure)
// are enforced at exactly one place. Thread-safe.
#include "congestion_fabric/backpressure.hpp"
#include "congestion_fabric/domain.hpp"
#include "congestion_fabric/fairness.hpp"
#include "congestion_fabric/flow.hpp"
#include "congestion_fabric/types.hpp"
#include <cstdint>
#include <limits>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

namespace congfabric {

struct FlowOptions;

// ---- structured result types ------------------------------------------------
struct CongestionEvidence {
  std::string name;   // e.g. "queue_growth", "offered_exceeds_serviced"
  double value{0.0};
  std::string detail;
};

struct CongestionAssessment {
  CongestionState state{CongestionState::UNKNOWN};
  MeasurementProvenance provenance{MeasurementProvenance::UNKNOWN};
  double score{0.0};
  bool authoritative{false};   // true only when evidence is fresh
  std::vector<CongestionEvidence> evidence;
};

struct BottleneckResult {
  EntityRef<CongestionDomain> domain;
  std::string cause;
  double deficit_bps{0.0};
  bool upstream{false};
  std::string explanation;      // deterministic structured explanation
};

struct ResidualCapacityResult {
  double effective_bps{0.0};
  double residual_bps{0.0};
  double utilised_bps{0.0};
  double utilization{0.0};
  BandwidthKind kind{BandwidthKind::UNKNOWN};
  MeasurementProvenance provenance{MeasurementProvenance::UNKNOWN};
  bool has_capacity{false};
};

struct FairnessReport {
  CongestionDomainId domain;
  FairnessModel model{FairnessModel::UNKNOWN};
  std::vector<FairnessShare> shares;
};

struct DomainSnapshot {
  EntityRef<CongestionDomain> ref;
  DomainKind kind{DomainKind::UNKNOWN};
  CongestionState state{CongestionState::UNKNOWN};
  MeasurementProvenance provenance{MeasurementProvenance::UNKNOWN};
  double offered_bps{0.0};
  double serviced_bps{0.0};
  double effective_bps{0.0};
  double residual_bps{0.0};
  double utilization{0.0};
  std::uint64_t queued_bytes{0};
  std::uint64_t queued_ops{0};
  std::uint64_t in_flight_bytes{0};
  FlowId dominant_contributor;
  TrafficClass dominant_class{TrafficClass::UNKNOWN};
  CongestionDomainId upstream_bottleneck;
  std::string summary;
};

// Bounded resource limits enforced before any externally supplied count causes
// unbounded allocation.
struct EngineLimits {
  std::size_t max_domains{4096};
  std::size_t max_active_flows{200000};
  std::size_t max_flow_history{1000000};
  std::size_t max_episode_history_per_domain{256};
  std::size_t max_queue_depth_ops{1u << 30};
  std::uint64_t max_queued_bytes{std::numeric_limits<std::uint64_t>::max() / 2};
  std::size_t max_path_length{64};
  std::size_t max_explanation_chars{4096};
};

class CongestionFabric {
 public:
  explicit CongestionFabric(EngineLimits limits = {});
  ~CongestionFabric();
  CongestionFabric(CongestionFabric&&) noexcept;
  CongestionFabric& operator=(CongestionFabric&&) noexcept;
  CongestionFabric(const CongestionFabric&) = delete;
  CongestionFabric& operator=(const CongestionFabric&) = delete;

  const EngineLimits& limits() const noexcept { return limits_; }

  // ---- generation / epoch authority ----------------------------------------
  CoordinatorEpoch epoch() const;
  bool advance_epoch(CoordinatorEpoch next);          // strict increase only
  bool register_worker(WorkerId w, WorkerBootId boot);
  bool register_source(SourceId s, SourceBootId boot);
  // Fence a worker that died. All live flows it exclusively owned become
  // STALE / REVALIDATION_REQUIRED; nothing is auto-completed.
  bool fence_worker(WorkerId w, WorkerBootId dead_boot);
  bool fence_source(SourceId s, SourceBootId dead_boot);

  // ---- domains ---------------------------------------------------------------
  EntityRef<CongestionDomain> create_domain(DomainKind kind,
                                            const CapacityProfile& capacity);
  bool remove_domain(CongestionDomainId id);
  const CongestionDomain* domain(CongestionDomainId id) const;
  std::size_t domain_count() const;
  std::vector<CongestionDomainId> domain_ids() const;

  // ---- flows (guarded lifecycle, exact accounting, authority check) ----------
  EntityRef<Flow> declare_flow(const FlowOptions& opts);
  bool admit_flow(const EntityRef<Flow>& ref);
  bool queue_flow(const EntityRef<Flow>& ref);
  bool start_flow(const EntityRef<Flow>& ref, WorkerBootId boot);
  bool flow_progress(const EntityRef<Flow>& ref, WorkerBootId boot,
                     std::uint64_t completed_delta);
  bool flow_complete(const EntityRef<Flow>& ref, WorkerBootId boot);
  bool flow_cancel(const EntityRef<Flow>& ref, WorkerBootId boot,
                   std::uint64_t cancelled_so_far);
  bool flow_fail(const EntityRef<Flow>& ref, WorkerBootId boot);
  bool advance_flow_generation(const EntityRef<Flow>& ref, FlowGeneration next);
  bool revalidate_flow(const EntityRef<Flow>& ref, WorkerBootId boot);
  const Flow* flow(FlowId id) const;
  std::uint64_t live_flow_count() const;

  // ---- measurements -----------------------------------------------------------
  bool publish_measurement(CongestionDomainId domain, Measurement m);
  // Authority-gated publication: the owning worker must be the current boot for
  // its WorkerId, otherwise the measurement is rejected as stale. Records which
  // worker owns the current offered/serviced aggregate so it can be invalidated
  // when that worker dies.
  bool publish_measurement(CongestionDomainId domain, Measurement m, WorkerId owner,
                           WorkerBootId owner_boot);

  // ---- congestion analysis -----------------------------------------------------
  CongestionAssessment assess_congestion(CongestionDomainId domain) const;
  // Computes the assessment AND persists it into the domain (cached state +
  // episode open/close boundaries). This is the mutation a caller invokes when
  // it wants the domain's cached congestion state advanced.
  CongestionAssessment recompute_domain(CongestionDomainId domain);
  BottleneckResult identify_bottleneck(std::vector<CongestionDomainId> path) const;
  ResidualCapacityResult query_residual_capacity(CongestionDomainId domain,
                                                 double offered_bps) const;
  FairnessReport evaluate_fairness(CongestionDomainId domain) const;
  BackpressureAction eval_backpressure(CongestionDomainId domain) const;
  bool apply_local_shaping(CongestionDomainId domain, const BackpressureAction& action);
  DomainSnapshot snapshot(CongestionDomainId domain) const;
  const CongestionEpisode* current_episode(CongestionDomainId domain) const;
  std::vector<CongestionEpisode> episode_history(CongestionDomainId domain) const;

  // ---- conservative recovery ----------------------------------------------------
  void recover_dynamic_evidence();   // mark dynamic facts stale / REVALIDATION_REQUIRED

  // ---- persistence ------------------------------------------------------------
  bool save(const std::string& path) const;
  bool load(const std::string& path);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
  EngineLimits limits_;
};

// Options used to declare a flow.
struct FlowOptions {
  WorkerId owner_worker;
  WorkerBootId owner_boot;
  SourceId source;
  SourceBootId source_boot;
  EntityRef<Workload> workload;
  EntityRef<Execution> execution;
  TrafficClass traffic_class{TrafficClass::UNKNOWN};
  EntityRef<CongestionDomain> domain;
  EntityRef<Path> path;
  std::uint64_t expected_bytes{0};
  std::uint64_t submitted_bytes{0};
  double priority{0.0};
  double latency_sensitivity{0.0};
  double throughput_target_bps{0.0};
  double burst_allowance_bytes{0.0};
  double deadline_ms{0.0};
  FairnessConfig fairness;
  MeasurementProvenance provenance{MeasurementProvenance::UNKNOWN};
  std::string description;
};

}  // namespace congfabric
