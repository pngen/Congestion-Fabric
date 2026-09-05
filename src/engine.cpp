#include "congestion_fabric/engine.hpp"
#include "congestion_fabric/checked.hpp"
#include "congestion_fabric/persistence.hpp"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <deque>
#include <fstream>
#include <limits>
#include <string>
#include <unordered_map>
#include <utility>

namespace congfabric {

namespace {

std::uint64_t now_ms() noexcept {
  using namespace std::chrono;
  return static_cast<std::uint64_t>(
      duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count());
}

// ------- persistence primitive (de)serializers -------------------------------
template <class T>
void wr(persist::Writer& w, const Id<T>& id) { w.u64(id.value()); }
template <class T>
void wr(persist::Writer& w, const Generation<T>& g) { w.u64(g.value()); }
template <class T>
void rd(persist::Reader& r, Id<T>& id) { id = Id<T>(r.u64()); }
template <class T>
void rd(persist::Reader& r, Generation<T>& g) { g = Generation<T>(r.u64()); }

void wr_rate(persist::Writer& w, const Rate& x) {
  w.u8(to_u8(x.kind));
  w.u8(to_u8(x.provenance));
  w.f64(x.bytes_per_second);
  wr(w, x.generation);
}
void rd_rate(persist::Reader& r, Rate& x) {
  x.kind = static_cast<BandwidthKind>(r.u8());
  if (!bandwidth_kind_valid(static_cast<std::uint8_t>(x.kind)))
    throw persist::CorruptError("invalid bandwidth kind");
  x.provenance = static_cast<MeasurementProvenance>(r.u8());
  if (!provenance_valid(static_cast<std::uint8_t>(x.provenance)))
    throw persist::CorruptError("invalid bandwidth provenance");
  x.bytes_per_second = r.f64();
  rd(r, x.generation);
  if (!x.is_finite_nonneg()) throw persist::CorruptError("non-finite bandwidth");
}

void wr_capacity(persist::Writer& w, const CapacityProfile& c) {
  wr_rate(w, c.theoretical());
  wr_rate(w, c.configured());
  wr_rate(w, c.measured_service());
  wr_rate(w, c.reserved());
  wr(w, c.generation());
}

void wr_fairness(persist::Writer& w, const FairnessConfig& f) {
  w.u8(to_u8(f.model));
  w.f64(f.weight);
  w.f64(f.minimum_guarantee_bps);
  w.f64(f.cap_bps);
  w.u8(f.latency_critical_protected ? 1 : 0);
  wr(w, f.policy_generation);
}

FlowState rd_flow_state(persist::Reader& r) {
  std::uint8_t u = r.u8();
  if (!flow_state_valid(u)) throw persist::CorruptError("invalid flow state");
  return static_cast<FlowState>(u);
}
DomainKind rd_domain_kind(persist::Reader& r) {
  std::uint8_t u = r.u8();
  if (!domain_kind_valid(u)) throw persist::CorruptError("invalid domain kind");
  return static_cast<DomainKind>(u);
}
TrafficClass rd_traffic_class(persist::Reader& r) {
  std::uint8_t u = r.u8();
  if (!traffic_class_valid(u)) throw persist::CorruptError("invalid traffic class");
  return static_cast<TrafficClass>(u);
}
MeasurementProvenance rd_prov(persist::Reader& r) {
  std::uint8_t u = r.u8();
  if (!provenance_valid(u)) throw persist::CorruptError("invalid provenance");
  return static_cast<MeasurementProvenance>(u);
}

void wr_measurement(persist::Writer& w, const Measurement& m) {
  w.u8(to_u8(m.kind));
  w.f64(m.value);
  w.u8(to_u8(m.provenance));
  wr(w, m.generation);
  w.u64(m.timestamp_ms);
  w.f64(m.confidence);
}
Measurement rd_measurement(persist::Reader& r) {
  Measurement m;
  std::uint8_t k = r.u8();
  if (!measurement_kind_valid(k)) throw persist::CorruptError("invalid measurement kind");
  m.kind = static_cast<MeasurementKind>(k);
  m.value = r.f64();
  m.provenance = rd_prov(r);
  rd(r, m.generation);
  m.timestamp_ms = r.u64();
  m.confidence = r.f64();
  return m;
}

// Byte counters are serialized as raw u64 and validated on load.
void wr_bytes(persist::Writer& w, const checked::ByteCounter& b) { w.u64(b.value()); }
checked::ByteCounter rd_bytes(persist::Reader& r) { return checked::ByteCounter(r.u64()); }

// ------- flow/domain (de)serialization -----------------------------------------
void wr_flow(persist::Writer& w, const Flow& f) {
  wr(w, f.ref.id);
  wr(w, f.ref.generation);
  wr(w, f.owner_worker);
  wr(w, f.owner_boot);
  wr(w, f.source);
  wr(w, f.source_boot);
  wr(w, f.workload.id);
  wr(w, f.workload.generation);
  wr(w, f.execution.id);
  wr(w, f.execution.generation);
  w.u8(to_u8(f.traffic_class));
  wr(w, f.domain.id);
  wr(w, f.domain.generation);
  wr(w, f.path.id);
  wr(w, f.path.generation);
  w.u8(to_u8(f.state));
  wr_bytes(w, f.expected_bytes);
  wr_bytes(w, f.submitted_bytes);
  wr_bytes(w, f.admitted_bytes);
  wr_bytes(w, f.in_flight_bytes);
  wr_bytes(w, f.completed_bytes);
  wr_bytes(w, f.failed_bytes);
  wr_bytes(w, f.cancelled_bytes);
  w.u64(f.declared_ms);
  w.u64(f.started_ms);
  w.u64(f.last_progress_ms);
  w.f64(f.priority);
  w.f64(f.latency_sensitivity);
  w.f64(f.throughput_target_bps);
  w.f64(f.burst_allowance_bytes);
  w.f64(f.deadline_ms);
  wr_fairness(w, f.fairness);
  w.u8(to_u8(f.provenance));
  w.str(f.description);
}

Flow rd_flow(persist::Reader& r) {
  Flow f;
  rd(r, f.ref.id);
  rd(r, f.ref.generation);
  rd(r, f.owner_worker);
  rd(r, f.owner_boot);
  rd(r, f.source);
  rd(r, f.source_boot);
  rd(r, f.workload.id);
  rd(r, f.workload.generation);
  rd(r, f.execution.id);
  rd(r, f.execution.generation);
  f.traffic_class = rd_traffic_class(r);
  rd(r, f.domain.id);
  rd(r, f.domain.generation);
  rd(r, f.path.id);
  rd(r, f.path.generation);
  f.state = rd_flow_state(r);
  f.expected_bytes = rd_bytes(r);
  f.submitted_bytes = rd_bytes(r);
  f.admitted_bytes = rd_bytes(r);
  f.in_flight_bytes = rd_bytes(r);
  f.completed_bytes = rd_bytes(r);
  f.failed_bytes = rd_bytes(r);
  f.cancelled_bytes = rd_bytes(r);
  f.declared_ms = r.u64();
  f.started_ms = r.u64();
  f.last_progress_ms = r.u64();
  f.priority = r.f64();
  f.latency_sensitivity = r.f64();
  f.throughput_target_bps = r.f64();
  f.burst_allowance_bytes = r.f64();
  f.deadline_ms = r.f64();
  f.fairness = FairnessConfig{};
  f.fairness.model = static_cast<FairnessModel>(r.u8());
  f.fairness.weight = r.f64();
  f.fairness.minimum_guarantee_bps = r.f64();
  f.fairness.cap_bps = r.f64();
  f.fairness.latency_critical_protected = r.u8() != 0;
  rd(r, f.fairness.policy_generation);
  f.provenance = rd_prov(r);
  f.description = r.str();
  return f;
}

void wr_episode(persist::Writer& w, const CongestionEpisode& e) {
  wr(w, e.event_id);
  wr(w, e.generation);
  wr(w, e.domain.id);
  wr(w, e.domain.generation);
  w.u8(to_u8(e.peak_state));
  w.u64(e.start_ms);
  w.u64(e.peak_ms);
  w.u64(e.recovery_start_ms);
  w.u64(e.end_ms);
  w.str(e.trigger);
  w.f64(e.peak_backlog_bytes);
  w.f64(e.service_deficit_bytes);
  w.u64(static_cast<std::uint64_t>(e.affected_flows.size()));
  for (const FlowId& f : e.affected_flows) wr(w, f);
  wr(w, e.bottleneck.id);
  wr(w, e.bottleneck.generation);
  w.u8(to_u8(e.outcome));
  w.u8(e.open ? 1 : 0);
}

void wr_domain(persist::Writer& w, const CongestionDomain& d) {
  wr(w, d.ref.id);
  wr(w, d.ref.generation);
  w.u8(to_u8(d.kind));
  wr_capacity(w, d.capacity);
  wr(w, d.queue.generation);
  w.u8(to_u8(d.state));
  w.u8(to_u8(d.state_provenance));
  wr(w, d.backpressure_generation);
  wr(w, d.recovery_generation);
  wr_fairness(w, d.default_fairness);
  w.f64(d.shaping.rate());
  w.f64(d.shaping.capacity());
  w.f64(d.offered_bps);
  w.u8(to_u8(d.offered_provenance));
  w.f64(d.admitted_bps);
  w.u8(to_u8(d.admitted_provenance));
  w.f64(d.serviced_bps);
  w.u8(to_u8(d.serviced_provenance));
  w.f64(d.utilisation);
  w.u8(to_u8(d.utilisation_provenance));
  w.f64(d.effective_bps);
  w.u8(to_u8(d.effective_provenance));
  w.f64(d.residual_bps);
  w.u8(to_u8(d.residual_provenance));
  w.u64(static_cast<std::uint64_t>(d.flows.size()));
  for (const EntityRef<Flow>& fr : d.flows) {
    wr(w, fr.id);
    wr(w, fr.generation);
  }
  w.u64(static_cast<std::uint64_t>(d.episodes.size()));
  for (const CongestionEpisode& e : d.episodes) wr_episode(w, e);
  wr_episode(w, d.current_episode);
}

}  // namespace

// ---------------------------------------------------------------------------
// Pimpl
// ---------------------------------------------------------------------------
struct CongestionFabric::Impl {
  mutable std::shared_mutex mtx;
  CoordinatorEpoch epoch_;
  std::unordered_map<WorkerId, WorkerBootId> worker_boot_;
  std::unordered_map<SourceId, SourceBootId> source_boot_;
  std::unordered_map<CongestionDomainId, CongestionDomain> domains_;
  std::unordered_map<FlowId, Flow> flows_;
  std::deque<FlowId> flow_history_;
  std::uint64_t next_domain_{1};
  std::uint64_t next_flow_{1};
  std::uint64_t next_event_{1};
  std::uint64_t next_policy_{1};
  std::uint64_t active_flows_{0};  // indexed count, avoids O(N) per declare
  bool recovered_{false};
};

CongestionFabric::CongestionFabric(EngineLimits limits) : impl_(new Impl), limits_(limits) {
  impl_->epoch_ = CoordinatorEpoch(1);
}
CongestionFabric::CongestionFabric(CongestionFabric&&) noexcept = default;
CongestionFabric& CongestionFabric::operator=(CongestionFabric&&) noexcept = default;
CongestionFabric::~CongestionFabric() = default;

CoordinatorEpoch CongestionFabric::epoch() const {
  std::shared_lock lk(impl_->mtx);
  return impl_->epoch_;
}

bool CongestionFabric::advance_epoch(CoordinatorEpoch next) {
  std::unique_lock lk(impl_->mtx);
  if (!next.is_valid() || next.value() <= impl_->epoch_.value()) return false;
  impl_->epoch_ = next;
  return true;
}

bool CongestionFabric::register_worker(WorkerId w, WorkerBootId boot) {
  if (!w.is_valid() || !boot.is_valid()) return false;
  std::unique_lock lk(impl_->mtx);
  auto it = impl_->worker_boot_.find(w);
  if (it != impl_->worker_boot_.end() && it->second.value() >= boot.value())
    return false;
  impl_->worker_boot_[w] = boot;
  return true;
}

bool CongestionFabric::register_source(SourceId s, SourceBootId boot) {
  if (!s.is_valid() || !boot.is_valid()) return false;
  std::unique_lock lk(impl_->mtx);
  auto it = impl_->source_boot_.find(s);
  if (it != impl_->source_boot_.end() && it->second.value() >= boot.value())
    return false;
  impl_->source_boot_[s] = boot;
  return true;
}

bool CongestionFabric::fence_worker(WorkerId w, WorkerBootId dead_boot) {
  if (!w.is_valid()) return false;
  std::unique_lock lk(impl_->mtx);
  auto bit = impl_->worker_boot_.find(w);
  if (bit == impl_->worker_boot_.end()) return false;
  if (bit->second != dead_boot) return false;
  bit->second = bit->second.next();
  for (auto& [fid, f] : impl_->flows_) {
    (void)fid;
    if (f.owner_worker == w && f.owner_boot == dead_boot && !f.is_terminal() &&
        !flow_is_stale_or_superseded(f.state)) {
      f.state = FlowState::STALE;
    }
  }
  // Invalidate worker-owned live domain measurements so the dead worker's
  // offered/serviced evidence cannot remain authoritative. The domain becomes
  // REVALIDATION_REQUIRED (never silently healthy) because a worker died and its
  // dynamic evidence must be republished before current congestion is asserted.
  bool any_invalidated = false;
  for (auto& [did, d] : impl_->domains_) {
    (void)did;
    bool owns_offered = d.offered_owner.is_valid() && d.offered_owner == w &&
                        d.offered_owner_boot == dead_boot;
    bool owns_serviced = d.serviced_owner.is_valid() && d.serviced_owner == w &&
                         d.serviced_owner_boot == dead_boot;
    if (owns_offered) {
      d.offered_bps = 0.0;
      d.offered_provenance = MeasurementProvenance::UNKNOWN;
      d.offered_owner = WorkerId(0);
      any_invalidated = true;
    }
    if (owns_serviced) {
      d.serviced_bps = 0.0;
      d.serviced_provenance = MeasurementProvenance::UNKNOWN;
      d.serviced_owner = WorkerId(0);
      any_invalidated = true;
    }
    if (any_invalidated) {
      d.queue.generation = d.queue.generation.next();
      if (d.state != CongestionState::UNKNOWN) {
        d.state = CongestionState::REVALIDATION_REQUIRED;
        d.state_provenance = MeasurementProvenance::UNKNOWN;
      }
    }
  }
  return true;
}

bool CongestionFabric::fence_source(SourceId s, SourceBootId dead_boot) {
  if (!s.is_valid()) return false;
  std::unique_lock lk(impl_->mtx);
  auto bit = impl_->source_boot_.find(s);
  if (bit == impl_->source_boot_.end()) return false;
  if (bit->second != dead_boot) return false;
  bit->second = bit->second.next();
  for (auto& [fid, f] : impl_->flows_) {
    (void)fid;
    if (f.source == s && f.source_boot == dead_boot && !f.is_terminal() &&
        !flow_is_stale_or_superseded(f.state)) {
      f.state = FlowState::STALE;
    }
  }
  return true;
}

EntityRef<CongestionDomain> CongestionFabric::create_domain(DomainKind kind,
                                                            const CapacityProfile& capacity) {
  std::unique_lock lk(impl_->mtx);
  EntityRef<CongestionDomain> invalid;
  if (impl_->domains_.size() >= limits_.max_domains) return invalid;
  if (!capacity.valid()) return invalid;
  CongestionDomainId id(impl_->next_domain_++);
  CongestionDomain d;
  d.ref.id = id;
  d.ref.generation = Generation<CongestionDomainTag>(1);
  d.kind = kind;
  d.capacity = capacity;
  d.queue.generation = QueueGeneration(1);
  d.state = CongestionState::UNKNOWN;
  d.state_provenance = MeasurementProvenance::UNKNOWN;
  d.residual_bps = capacity.residual(0.0).value_or(0.0);
  impl_->domains_.emplace(id, std::move(d));
  return impl_->domains_.find(id)->second.ref;
}

bool CongestionFabric::remove_domain(CongestionDomainId id) {
  std::unique_lock lk(impl_->mtx);
  return impl_->domains_.erase(id) != 0;
}

const CongestionDomain* CongestionFabric::domain(CongestionDomainId id) const {
  std::shared_lock lk(impl_->mtx);
  auto it = impl_->domains_.find(id);
  return it == impl_->domains_.end() ? nullptr : &it->second;
}

std::size_t CongestionFabric::domain_count() const {
  std::shared_lock lk(impl_->mtx);
  return impl_->domains_.size();
}

std::vector<CongestionDomainId> CongestionFabric::domain_ids() const {
  std::shared_lock lk(impl_->mtx);
  std::vector<CongestionDomainId> out;
  out.reserve(impl_->domains_.size());
  for (const auto& [id, d] : impl_->domains_) {
    (void)d;
    out.push_back(id);
  }
  return out;
}

EntityRef<Flow> CongestionFabric::declare_flow(const FlowOptions& opts) {
  std::unique_lock lk(impl_->mtx);
  EntityRef<Flow> invalid;
  if (!opts.owner_worker.is_valid() || !opts.owner_boot.is_valid()) return invalid;
  if (!opts.source.is_valid() || !opts.source_boot.is_valid()) return invalid;
  auto wit = impl_->worker_boot_.find(opts.owner_worker);
  if (wit == impl_->worker_boot_.end() || wit->second != opts.owner_boot)
    return invalid;
  auto sit = impl_->source_boot_.find(opts.source);
  if (sit == impl_->source_boot_.end() || sit->second != opts.source_boot)
    return invalid;
  if (!opts.domain.id.is_valid()) return invalid;
  if (impl_->domains_.find(opts.domain.id) == impl_->domains_.end()) return invalid;

  if (impl_->active_flows_ >= static_cast<std::uint64_t>(limits_.max_active_flows)) return invalid;

  Flow f;
  FlowId id(impl_->next_flow_++);
  f.ref.id = id;
  f.ref.generation = FlowGeneration(1);
  f.owner_worker = opts.owner_worker;
  f.owner_boot = opts.owner_boot;
  f.source = opts.source;
  f.source_boot = opts.source_boot;
  f.workload = opts.workload;
  f.execution = opts.execution;
  f.traffic_class = opts.traffic_class;
  f.domain = opts.domain;
  f.path = opts.path;
  f.state = FlowState::DECLARED;
  f.expected_bytes = checked::ByteCounter(opts.expected_bytes);
  f.submitted_bytes = checked::ByteCounter(opts.submitted_bytes);
  f.priority = opts.priority;
  f.latency_sensitivity = opts.latency_sensitivity;
  f.throughput_target_bps = opts.throughput_target_bps;
  f.burst_allowance_bytes = opts.burst_allowance_bytes;
  f.deadline_ms = opts.deadline_ms;
  f.fairness = opts.fairness;
  f.provenance = opts.provenance;
  f.description = opts.description;
  f.declared_ms = now_ms();
  impl_->flows_.emplace(id, std::move(f));
  ++impl_->active_flows_;
  impl_->flow_history_.push_back(id);
  if (impl_->flow_history_.size() > limits_.max_flow_history)
    impl_->flow_history_.pop_front();
  return impl_->flows_.find(id)->second.ref;
}

namespace {
const Flow* find_flow(const std::unordered_map<FlowId, Flow>& fm,
                      const EntityRef<Flow>& ref) {
  auto it = fm.find(ref.id);
  if (it == fm.end()) return nullptr;
  if (it->second.ref.generation != ref.generation) return nullptr;
  return &it->second;
}
Flow* find_flow_mut(std::unordered_map<FlowId, Flow>& fm,
                    const EntityRef<Flow>& ref) {
  auto it = fm.find(ref.id);
  if (it == fm.end()) return nullptr;
  if (it->second.ref.generation != ref.generation) return nullptr;
  return &it->second;
}
}  // namespace

bool CongestionFabric::admit_flow(const EntityRef<Flow>& ref) {
  std::unique_lock lk(impl_->mtx);
  Flow* f = find_flow_mut(impl_->flows_, ref);
  if (!f) return false;
  if (!flow_transition_allowed(f->state, FlowState::ADMITTED)) return false;
  auto dit = impl_->domains_.find(f->domain.id);
  if (dit == impl_->domains_.end()) return false;
  if (!f->admitted_bytes.add(f->submitted_bytes.value())) return false;
  if (f->admitted_bytes.value() > f->submitted_bytes.value()) return false;
  f->state = FlowState::ADMITTED;
  CongestionDomain& d = dit->second;
  // A flow is bound to its domain exactly once at admission; the guarded
  // lifecycle prevents re-admission, so the append is O(1) (no duplicate scan).
  d.flows.push_back(ref);
  return true;
}

bool CongestionFabric::queue_flow(const EntityRef<Flow>& ref) {
  std::unique_lock lk(impl_->mtx);
  Flow* f = find_flow_mut(impl_->flows_, ref);
  if (!f) return false;
  if (!flow_transition_allowed(f->state, FlowState::QUEUED)) return false;
  auto dit = impl_->domains_.find(f->domain.id);
  if (dit == impl_->domains_.end()) return false;
  CongestionDomain& d = dit->second;
  std::uint64_t queued = 0;
  for (const EntityRef<Flow>& r : d.flows) {
    if (const Flow* q = find_flow(impl_->flows_, r)) {
      if (q->state == FlowState::QUEUED) queued += 1;
    }
  }
  if (queued + 1 > static_cast<std::uint64_t>(limits_.max_queue_depth_ops))
    return false;
  f->state = FlowState::QUEUED;
  return true;
}

bool CongestionFabric::start_flow(const EntityRef<Flow>& ref, WorkerBootId boot) {
  std::unique_lock lk(impl_->mtx);
  Flow* f = find_flow_mut(impl_->flows_, ref);
  if (!f) return false;
  if (f->owner_boot != boot) return false;
  if (flow_is_stale_or_superseded(f->state)) return false;
  if (f->state != FlowState::QUEUED && f->state != FlowState::ADMITTED &&
      f->state != FlowState::VALIDATING)
    return false;
  f->state = FlowState::ACTIVE;
  if (!f->in_flight_bytes.add(f->admitted_bytes.value())) return false;
  if (f->in_flight_bytes.value() > f->admitted_bytes.value()) return false;
  f->started_ms = now_ms();
  f->last_progress_ms = f->started_ms;
  auto dit = impl_->domains_.find(f->domain.id);
  if (dit != impl_->domains_.end()) {
    CongestionDomain& d = dit->second;
    if (d.state == CongestionState::REVALIDATION_REQUIRED || d.state == CongestionState::STALE)
      d.state = CongestionState::UNKNOWN;
  }
  return true;
}

bool CongestionFabric::flow_progress(const EntityRef<Flow>& ref, WorkerBootId boot,
                                     std::uint64_t completed_delta) {
  std::unique_lock lk(impl_->mtx);
  Flow* f = find_flow_mut(impl_->flows_, ref);
  if (!f) return false;
  if (f->owner_boot != boot) return false;
  if (flow_is_stale_or_superseded(f->state)) return false;
  if (!flow_can_progress(f->state)) return false;
  std::uint64_t new_completed = 0;
  if (!checked::add(f->completed_bytes.value(), completed_delta, new_completed))
    return false;
  if (new_completed > f->admitted_bytes.value()) return false;
  std::uint64_t moved = std::min(completed_delta, f->in_flight_bytes.value());
  std::uint64_t new_inflight = 0;
  if (!checked::sub(f->in_flight_bytes.value(), moved, new_inflight)) return false;
  f->completed_bytes = checked::ByteCounter(new_completed);
  f->in_flight_bytes = checked::ByteCounter(new_inflight);
  f->last_progress_ms = now_ms();
  return true;
}

bool CongestionFabric::flow_complete(const EntityRef<Flow>& ref, WorkerBootId boot) {
  std::unique_lock lk(impl_->mtx);
  Flow* f = find_flow_mut(impl_->flows_, ref);
  if (!f) return false;
  if (f->owner_boot != boot) return false;
  if (f->state == FlowState::CANCELLED || f->state == FlowState::FAILED ||
      f->state == FlowState::COMPLETED)
    return false;
  if (!flow_transition_allowed(f->state, FlowState::COMPLETED)) return false;
  std::uint64_t rem = f->admitted_bytes.value() - f->completed_bytes.value();
  if (!f->completed_bytes.add(rem)) return false;
  if (f->completed_bytes.value() > f->admitted_bytes.value()) return false;
  f->in_flight_bytes = checked::ByteCounter(0);
  f->state = FlowState::COMPLETED;
  f->last_progress_ms = now_ms();
  if (impl_->active_flows_ > 0) --impl_->active_flows_;
  return true;
}

bool CongestionFabric::flow_cancel(const EntityRef<Flow>& ref, WorkerBootId boot,
                                   std::uint64_t cancelled_so_far) {
  std::unique_lock lk(impl_->mtx);
  Flow* f = find_flow_mut(impl_->flows_, ref);
  if (!f) return false;
  if (f->owner_boot != boot) return false;
  if (flow_is_stale_or_superseded(f->state)) return false;
  if (!flow_transition_allowed(f->state, FlowState::CANCELLED)) return false;
  std::uint64_t remaining = f->admitted_bytes.value() - f->completed_bytes.value();
  std::uint64_t canc = std::min(cancelled_so_far, remaining);
  if (!f->cancelled_bytes.add(canc)) return false;
  if (f->completed_bytes.value() + f->cancelled_bytes.value() >
      f->admitted_bytes.value())
    return false;
  f->in_flight_bytes = checked::ByteCounter(0);
  f->state = FlowState::CANCELLED;
  f->last_progress_ms = now_ms();
  if (impl_->active_flows_ > 0) --impl_->active_flows_;
  return true;
}

bool CongestionFabric::flow_fail(const EntityRef<Flow>& ref, WorkerBootId boot) {
  std::unique_lock lk(impl_->mtx);
  Flow* f = find_flow_mut(impl_->flows_, ref);
  if (!f) return false;
  if (f->owner_boot != boot) return false;
  if (!flow_transition_allowed(f->state, FlowState::FAILED)) return false;
  std::uint64_t remaining = f->admitted_bytes.value() - f->completed_bytes.value();
  if (!f->failed_bytes.add(remaining)) return false;
  f->in_flight_bytes = checked::ByteCounter(0);
  f->state = FlowState::FAILED;
  f->last_progress_ms = now_ms();
  if (impl_->active_flows_ > 0) --impl_->active_flows_;
  return true;
}

bool CongestionFabric::advance_flow_generation(const EntityRef<Flow>& ref,
                                               FlowGeneration next) {
  std::unique_lock lk(impl_->mtx);
  auto it = impl_->flows_.find(ref.id);
  if (it == impl_->flows_.end()) return false;
  if (!next.is_valid() || next.value() <= it->second.ref.generation.value())
    return false;
  it->second.ref.generation = next;
  if (it->second.state == FlowState::STALE)
    it->second.state = FlowState::REVALIDATION_REQUIRED;
  return true;
}

bool CongestionFabric::revalidate_flow(const EntityRef<Flow>& ref, WorkerBootId boot) {
  std::unique_lock lk(impl_->mtx);
  Flow* f = find_flow_mut(impl_->flows_, ref);
  if (!f) return false;
  if (f->owner_boot != boot) return false;
  if (!flow_transition_allowed(f->state, FlowState::REVALIDATION_REQUIRED))
    return false;
  f->state = FlowState::REVALIDATION_REQUIRED;
  return true;
}

const Flow* CongestionFabric::flow(FlowId id) const {
  std::shared_lock lk(impl_->mtx);
  auto it = impl_->flows_.find(id);
  return it == impl_->flows_.end() ? nullptr : &it->second;
}

std::uint64_t CongestionFabric::live_flow_count() const {
  std::shared_lock lk(impl_->mtx);
  return impl_->active_flows_;
}

bool CongestionFabric::publish_measurement(CongestionDomainId domain, Measurement m,
                                           WorkerId owner, WorkerBootId owner_boot) {
  if (!m.valid()) return false;
  std::unique_lock lk(impl_->mtx);
  auto it = impl_->domains_.find(domain);
  if (it == impl_->domains_.end()) return false;
  CongestionDomain& d = it->second;
  // Authority gate: an owned measurement must come from the current boot of its
  // worker. A stale boot is rejected so old worker evidence cannot be republished
  // as fresh after the worker was fenced / restarted.
  if (owner.is_valid()) {
    auto wit = impl_->worker_boot_.find(owner);
    if (wit == impl_->worker_boot_.end() || wit->second != owner_boot) return false;
  }
  // Fresh evidence republished after conservative recovery clears the
  // revalidation requirement so the domain can be re-evaluated.
  if (d.state == CongestionState::REVALIDATION_REQUIRED || d.state == CongestionState::STALE)
    d.state = CongestionState::UNKNOWN;
  d.measurements.put(m);
  switch (m.kind) {
    case MeasurementKind::OFFERED_BYTES_PER_SEC:
      d.offered_bps = m.value;
      d.offered_provenance = m.provenance;
      d.offered_owner = owner;
      d.offered_owner_boot = owner_boot;
      break;
    case MeasurementKind::ADMITTED_BYTES_PER_SEC:
      d.admitted_bps = m.value;
      d.admitted_provenance = m.provenance;
      break;
    case MeasurementKind::COMPLETED_BYTES_PER_SEC:
      d.serviced_bps = m.value;
      d.serviced_provenance = m.provenance;
      d.serviced_owner = owner;
      d.serviced_owner_boot = owner_boot;
      break;
    case MeasurementKind::UTILIZATION:
      d.utilisation = m.value;
      d.utilisation_provenance = m.provenance;
      break;
    case MeasurementKind::EFFECTIVE_BANDWIDTH:
      d.effective_bps = m.value;
      d.effective_provenance = m.provenance;
      break;
    default:
      break;
  }
  return true;
}

bool CongestionFabric::publish_measurement(CongestionDomainId domain, Measurement m) {
  // Non-authority API used by the engine API / tests: records the measurement as
  // unowned (never invalidated automatically).
  return publish_measurement(domain, m, WorkerId(0), WorkerBootId(0));
}

// The classify helper (deterministic, evidence-based).
namespace {
struct Aggregate {
  std::uint64_t queued_bytes{0}, queued_ops{0}, in_flight_bytes{0}, in_flight_ops{0};
  std::uint64_t completed_bytes{0}, cancelled_bytes{0}, failed_bytes{0};
  std::uint64_t active_flows{0};
};

Aggregate aggregate_domain(const CongestionDomain& d,
                           const std::unordered_map<FlowId, Flow>& flows) {
  Aggregate agg;
  for (const EntityRef<Flow>& fr : d.flows) {
    const Flow* f = find_flow(flows, fr);
    if (!f || flow_is_stale_or_superseded(f->state)) continue;
    if (f->state == FlowState::QUEUED) {
      agg.queued_ops += 1;
      agg.queued_bytes += f->admitted_bytes.value();
    } else if (flow_can_progress(f->state)) {
      agg.in_flight_ops += 1;
      agg.in_flight_bytes += f->in_flight_bytes.value();
    }
    agg.completed_bytes += f->completed_bytes.value();
    agg.cancelled_bytes += f->cancelled_bytes.value();
    agg.failed_bytes += f->failed_bytes.value();
    if (!f->is_terminal()) agg.active_flows += 1;
  }
  return agg;
}

CongestionAssessment classify(const CongestionDomain& d, const Aggregate& agg,
                              const std::unordered_map<FlowId, Flow>& flows,
                              bool revalidation) {
  CongestionAssessment a;
  double eff = 0.0;
  bool has_eff = false;
  if (auto e = d.capacity.effective()) {
    eff = *e;
    has_eff = true;
  }
  if (revalidation) {
    a.state = CongestionState::REVALIDATION_REQUIRED;
    a.provenance = MeasurementProvenance::UNKNOWN;
    a.authoritative = false;
    a.evidence.push_back({"revalidation_required", 0.0,
                          "Dynamic evidence not republished since recovery"});
    return a;
  }
  if (!has_eff) {
    a.state = CongestionState::UNKNOWN;
    a.provenance = MeasurementProvenance::UNKNOWN;
    a.authoritative = false;
    a.evidence.push_back({"no_capacity_bound", 0.0,
                          "No valid effective capacity to classify against"});
    return a;
  }

  double offered = d.offered_bps;
  double serviced = d.serviced_bps;
  bool no_traffic =
      offered <= 0.0 && serviced <= 0.0 && agg.queued_ops == 0 && agg.in_flight_ops == 0;
  if (no_traffic) {
    a.state = CongestionState::IDLE;
    a.authoritative = true;
    a.provenance = d.serviced_provenance;
    a.evidence.push_back({"idle", 0.0, "No offered or in-flight traffic"});
    return a;
  }

  double sat = eff > 0.0 ? offered / eff : 0.0;
  double serv_ratio = eff > 0.0 ? serviced / eff : 0.0;
  double deficit = offered - serviced;
  // Backlog (queued bytes) is the genuine queue signal. In-flight bytes are
  // active work, not queueing. Distinguish healthy high utilization from real
  // backlog/congestion.
  bool has_queue = agg.queued_bytes > 0;
  bool queue_growing = deficit > 1e-9;
  bool queue_draining = deficit < -1e-9 && has_queue;

  double latency_inflation = 1.0;
  if (const Measurement* lat = d.measurements.get(MeasurementKind::SERVICE_LATENCY);
      lat && lat->value > 0.0 && eff > 0.0) {
    latency_inflation = lat->value * eff;
  }
  bool latency_high = latency_inflation > 2.0;

  a.provenance =
      (d.serviced_provenance == MeasurementProvenance::MEASURED ||
       d.offered_provenance == MeasurementProvenance::MEASURED)
          ? MeasurementProvenance::MEASURED
          : (d.serviced_provenance == MeasurementProvenance::UNKNOWN
                 ? d.offered_provenance
                 : d.serviced_provenance);

  double score = 0.0;
  score += std::clamp(sat, 0.0, 1.0) * 0.40;
  score += std::clamp(serv_ratio, 0.0, 1.0) * 0.20;
  score += std::clamp(static_cast<double>(agg.queued_bytes) /
                          (eff > 0.0 ? eff : 1.0), 0.0, 1.0) * 0.20;
  if (deficit > 0.0) score += std::clamp(deficit / eff, 0.0, 1.0) * 0.20;
  a.score = std::clamp(score, 0.0, 1.0);
  a.authoritative = true;

  bool high_demand = sat >= 0.9;
  bool over_demand = sat > 1.0;
  bool saturated = serv_ratio >= 0.95 && high_demand;

  // Starvation: at least one live flow made no progress while another did.
  bool starvation = false;
  std::uint64_t max_completed = 0;
  bool any_live_flow = false;
  for (const EntityRef<Flow>& fr : d.flows) {
    const Flow* f = find_flow(flows, fr);
    if (!f || f->is_terminal() || flow_is_stale_or_superseded(f->state)) continue;
    any_live_flow = true;
    max_completed = std::max(max_completed, f->completed_bytes.value());
  }
  if (any_live_flow) {
    for (const EntityRef<Flow>& fr : d.flows) {
      const Flow* f = find_flow(flows, fr);
      if (!f || f->is_terminal() || flow_is_stale_or_superseded(f->state)) continue;
      if (f->completed_bytes.value() == 0 && f->in_flight_bytes.value() > 0 &&
          max_completed > 0) {
        starvation = true;
        break;
      }
    }
  }

  if (has_queue && queue_growing && (latency_high || starvation)) {
    a.state = CongestionState::SEVERELY_CONGESTED;
  } else if (has_queue && queue_growing && (deficit > 0.2 * eff || latency_high)) {
    a.state = CongestionState::CONGESTED;
  } else if (has_queue && queue_growing) {
    a.state = CongestionState::QUEUEING;
  } else if (over_demand && !has_queue) {
    a.state = CongestionState::SATURATED;
  } else if (saturated && !has_queue) {
    a.state = CongestionState::APPROACHING_SATURATION;
  } else if (has_queue && queue_draining) {
    a.state = CongestionState::RECOVERING;
  } else if (has_queue) {
    a.state = CongestionState::QUEUEING;
  } else if (sat >= 0.8) {
    a.state = CongestionState::BUSY;
  } else {
    a.state = CongestionState::HEALTHY;
  }

  auto note = [&](const char* name, double v, const char* detail) {
    a.evidence.push_back({name, v, detail});
  };
  note("offered_bps", offered, "Offered load");
  note("serviced_bps", serviced, "Serviced load");
  note("effective_bps", eff, "Effective capacity");
  note("queue_bytes", static_cast<double>(agg.queued_bytes), "Queued backlog");
  note("deficit_bps", deficit, "offered - serviced");
  note("starvation", starvation ? 1.0 : 0.0, "A live flow made no progress");
  return a;
}

}  // namespace

CongestionAssessment CongestionFabric::assess_congestion(CongestionDomainId domain) const {
  std::shared_lock lk(impl_->mtx);
  auto it = impl_->domains_.find(domain);
  if (it == impl_->domains_.end()) return CongestionAssessment{};
  bool reval = it->second.state == CongestionState::REVALIDATION_REQUIRED ||
               it->second.state == CongestionState::STALE;
  auto agg = aggregate_domain(it->second, impl_->flows_);
  return classify(it->second, agg, impl_->flows_, reval);
}

CongestionAssessment CongestionFabric::recompute_domain(CongestionDomainId domain) {
  std::unique_lock lk(impl_->mtx);
  auto it = impl_->domains_.find(domain);
  if (it == impl_->domains_.end()) return CongestionAssessment{};
  CongestionDomain& d = it->second;
  auto agg = aggregate_domain(d, impl_->flows_);
  bool reval = d.state == CongestionState::REVALIDATION_REQUIRED ||
               d.state == CongestionState::STALE;
  CongestionAssessment a = classify(d, agg, impl_->flows_, reval);
  d.state = a.state;
  d.state_provenance = a.provenance;

  bool congested_now =
      (a.state == CongestionState::CONGESTED ||
       a.state == CongestionState::SEVERELY_CONGESTED ||
       a.state == CongestionState::QUEUEING ||
       a.state == CongestionState::SATURATED ||
       a.state == CongestionState::APPROACHING_SATURATION);
  CongestionEpisode& ep = d.current_episode;
  if (congested_now && !ep.open) {
    ep = CongestionEpisode{};
    ep.event_id = CongestionEventId(impl_->next_event_++);
    ep.generation = CongestionEventGeneration(1);
    ep.domain = d.ref;
    ep.start_ms = now_ms();
    ep.peak_state = a.state;
    ep.trigger = "offered load exceeded effective capacity";
    ep.open = true;
  } else if (congested_now && ep.open) {
    ep.peak_state = a.state;
    ep.peak_backlog_bytes =
        std::max(ep.peak_backlog_bytes, static_cast<double>(agg.queued_bytes));
  } else if (!congested_now && ep.open) {
    ep.end_ms = now_ms();
    ep.outcome = a.state;
    ep.open = false;
    d.episodes.push_back(ep);
    if (d.episodes.size() > limits_.max_episode_history_per_domain)
      d.episodes.erase(d.episodes.begin());
    d.current_episode = CongestionEpisode{};
  }
  return a;
}

BottleneckResult CongestionFabric::identify_bottleneck(
    std::vector<CongestionDomainId> path) const {
  std::shared_lock lk(impl_->mtx);
  BottleneckResult best;
  best.domain.id = CongestionDomainId(0);
  best.deficit_bps = -1.0;
  for (CongestionDomainId id : path) {
    auto it = impl_->domains_.find(id);
    if (it == impl_->domains_.end()) continue;
    const CongestionDomain& d = it->second;
    auto agg = aggregate_domain(d, impl_->flows_);
    double eff = d.capacity.effective().value_or(0.0);
    double deficit = d.offered_bps - d.serviced_bps;
    double backlog = static_cast<double>(agg.queued_bytes);
    double pressure = deficit + backlog;
    if (pressure > best.deficit_bps || best.deficit_bps < 0.0) {
      best.deficit_bps = pressure;
      best.domain = d.ref;
      best.upstream = false;
      best.cause = "offered exceeds serviced; backlog growing";
      best.explanation =
          "Bottleneck at domain " + std::to_string(d.ref.id.value()) +
          " (" + std::to_string(static_cast<int>(d.kind)) +
          "): offered=" + std::to_string(d.offered_bps) +
          ", serviced=" + std::to_string(d.serviced_bps) +
          ", effective=" + std::to_string(eff) + ", backlog=" +
          std::to_string(backlog) + ".";
    }
  }
  if (best.deficit_bps < 0.0) {
    best.explanation = "No bottleneck: all path domains are within capacity.";
    best.deficit_bps = 0.0;
  }
  return best;
}

ResidualCapacityResult CongestionFabric::query_residual_capacity(
    CongestionDomainId domain, double offered_bps) const {
  std::shared_lock lk(impl_->mtx);
  ResidualCapacityResult r;
  auto it = impl_->domains_.find(domain);
  if (it == impl_->domains_.end()) return r;
  const CongestionDomain& d = it->second;
  auto eff = d.capacity.effective();
  if (!eff) return r;
  r.effective_bps = *eff;
  auto res = d.capacity.residual(offered_bps);
  r.residual_bps = res.value_or(0.0);
  r.utilised_bps = offered_bps;
  r.utilization = *eff > 0.0 ? offered_bps / *eff : 0.0;
  r.has_capacity = res.has_value();
  const Rate& m = d.capacity.measured_service();
  r.kind = m.kind != BandwidthKind::UNKNOWN
               ? m.kind
               : (d.capacity.configured().kind != BandwidthKind::UNKNOWN
                      ? d.capacity.configured().kind
                      : d.capacity.theoretical().kind);
  r.provenance = m.provenance;
  return r;
}

FairnessReport CongestionFabric::evaluate_fairness(CongestionDomainId domain) const {
  std::shared_lock lk(impl_->mtx);
  FairnessReport rep;
  rep.domain = domain;
  auto it = impl_->domains_.find(domain);
  if (it == impl_->domains_.end()) return rep;
  const CongestionDomain& d = it->second;
  rep.model = d.default_fairness.model;
  double eff = d.capacity.effective().value_or(0.0);

  // Gather live participants.
  std::vector<const Flow*> parts;
  std::uint64_t total_completed = 0;
  double total_weight = 0.0;
  for (const EntityRef<Flow>& fr : d.flows) {
    const Flow* f = find_flow(impl_->flows_, fr);
    if (!f || f->is_terminal() || flow_is_stale_or_superseded(f->state)) continue;
    parts.push_back(f);
    total_completed += f->completed_bytes.value();
    total_weight += (f->fairness.weight > 0.0 ? f->fairness.weight : 1.0);
  }
  if (parts.empty()) return rep;
  std::size_t n = parts.size();
  for (const Flow* f : parts) {
    FairnessShare s;
    s.flow = f->ref.id;
    s.flow_generation = f->ref.generation;
    s.config = f->fairness;
    s.config.model = d.default_fairness.model;
    double weight = f->fairness.weight > 0.0 ? f->fairness.weight : 1.0;
    double target = 0.0;
    switch (s.config.model) {
      case FairnessModel::EQUAL_SHARE:
        target = eff / static_cast<double>(n);
        break;
      case FairnessModel::WEIGHTED_SHARE:
      case FairnessModel::CALLER_WEIGHTS:
        target = total_weight > 0.0 ? eff * (weight / total_weight) : eff / static_cast<double>(n);
        break;
      case FairnessModel::MINIMUM_GUARANTEED_SHARE:
        target = std::max(s.config.minimum_guarantee_bps, eff / static_cast<double>(n));
        break;
      case FairnessModel::CAPPED_SHARE:
        target = std::min(eff / static_cast<double>(n),
                          s.config.cap_bps > 0.0 ? s.config.cap_bps : std::numeric_limits<double>::max());
        break;
      case FairnessModel::PROTECTED_LATENCY_CRITICAL_SHARE:
        target = eff / static_cast<double>(n);
        break;
      default:
        target = eff / static_cast<double>(n);
        break;
    }
    // Observed share: distribute serviced proportionally to completed bytes.
    double observed = total_completed > 0
                          ? d.serviced_bps * (static_cast<double>(f->completed_bytes.value()) /
                                              static_cast<double>(total_completed))
                          : d.serviced_bps / static_cast<double>(n);
    s.target_share_bps = target;
    s.observed_share_bps = observed;
    s.fairness_deficit_bps = std::max(0.0, target - observed);
    s.starved = eff > 0.0 && observed < 0.05 * eff && d.serviced_bps > 0.0;
    s.provenance = MeasurementProvenance::DERIVED;
    rep.shares.push_back(s);
  }
  return rep;
}

BackpressureAction CongestionFabric::eval_backpressure(CongestionDomainId domain) const {
  std::shared_lock lk(impl_->mtx);
  BackpressureAction action;
  auto it = impl_->domains_.find(domain);
  if (it == impl_->domains_.end()) return action;
  const CongestionDomain& d = it->second;
  int state = static_cast<int>(d.state);
  // SATURATED / QUEUEING / CONGESTED / ... all warrant bounded backpressure.
  if (state < static_cast<int>(CongestionState::SATURATED)) {
    action.intent = ActionIntent::NO_ACTION;
    return action;
  }
  double deficit = d.offered_bps - d.serviced_bps;
  if (deficit <= 0.0) {
    action.intent = ActionIntent::NO_ACTION;
    return action;
  }
  // Dominant contributor = largest in-flight flow.
  const Flow* dominant = nullptr;
  std::uint64_t max_inflight = 0;
  for (const EntityRef<Flow>& fr : d.flows) {
    const Flow* f = find_flow(impl_->flows_, fr);
    if (!f || f->is_terminal() || flow_is_stale_or_superseded(f->state)) continue;
    if (f->in_flight_bytes.value() > max_inflight) {
      max_inflight = f->in_flight_bytes.value();
      dominant = f;
    }
  }
  action.intent = (d.state == CongestionState::SEVERELY_CONGESTED)
                      ? ActionIntent::PAUSE_NEW_TRAFFIC
                      : (d.state == CongestionState::CONGESTED
                             ? ActionIntent::THROTTLE
                             : ActionIntent::ADMIT_LIMITED);
  if (dominant) {
    action.target_flow.id = dominant->ref.id;
    action.target_flow.generation = dominant->ref.generation;
  }
  action.target_domain = d.ref;
  action.cause = "offered exceeds effective capacity; deficit=" +
                 std::to_string(deficit);
  action.expected_benefit_bps = std::max(0.0, deficit);
  action.estimated_cost_bps = dominant ? dominant->throughput_target_bps : 0.0;
  action.cost_ms = 1.0;
  action.policy_generation = d.default_fairness.policy_generation;
  action.generation = d.backpressure_generation.next();
  action.provenance = d.serviced_provenance;
  action.epoch = impl_->epoch_;
  action.validity_window_ms = 1000.0;
  action.preconditions.push_back("flow generation current");
  action.preconditions.push_back("domain congestion current");
  return action;
}

bool CongestionFabric::apply_local_shaping(CongestionDomainId domain,
                                           const BackpressureAction& action) {
  if (!action.valid()) return false;
  if (!intent_is_restrictive(action.intent)) return true;  // no-op bounding
  std::unique_lock lk(impl_->mtx);
  auto it = impl_->domains_.find(domain);
  if (it == impl_->domains_.end()) return false;
  CongestionDomain& d = it->second;
  // Bounded local software shaping only: reduce the admitted envelope.
  double eff = d.capacity.effective().value_or(d.serviced_bps);
  double deficit = std::max(0.0, d.offered_bps - d.serviced_bps);
  double allowed = std::max(0.0, eff - deficit);
  d.shaping = TokenBucket(allowed, allowed);
  d.backpressure_generation = action.generation;
  return true;
}

DomainSnapshot CongestionFabric::snapshot(CongestionDomainId domain) const {
  std::shared_lock lk(impl_->mtx);
  DomainSnapshot s;
  auto it = impl_->domains_.find(domain);
  if (it == impl_->domains_.end()) return s;
  const CongestionDomain& d = it->second;
  s.ref = d.ref;
  s.kind = d.kind;
  bool reval = d.state == CongestionState::REVALIDATION_REQUIRED ||
               d.state == CongestionState::STALE;
  auto agg = aggregate_domain(d, impl_->flows_);
  CongestionAssessment a = classify(d, agg, impl_->flows_, reval);
  s.state = a.state;
  s.provenance = a.provenance;
  s.offered_bps = d.offered_bps;
  s.serviced_bps = d.serviced_bps;
  s.effective_bps = d.capacity.effective().value_or(0.0);
  s.residual_bps = d.capacity.residual(d.offered_bps).value_or(0.0);
  s.utilization = s.effective_bps > 0.0 ? d.offered_bps / s.effective_bps : 0.0;
  s.queued_bytes = agg.queued_bytes;
  s.queued_ops = agg.queued_ops;
  s.in_flight_bytes = agg.in_flight_bytes;

  // Dominant contributor (largest in-flight live flow).
  std::uint64_t max_inflight = 0;
  for (const EntityRef<Flow>& fr : d.flows) {
    const Flow* f = find_flow(impl_->flows_, fr);
    if (!f || f->is_terminal()) continue;
    if (f->in_flight_bytes.value() > max_inflight) {
      max_inflight = f->in_flight_bytes.value();
      s.dominant_contributor = f->ref.id;
      s.dominant_class = f->traffic_class;
    }
  }
  // Identify an upstream bottleneck if any upstream domain is more congested.
  for (CongestionDomainId up : d.upstream_domains) {
    auto uit = impl_->domains_.find(up);
    if (uit == impl_->domains_.end()) continue;
    const CongestionDomain& u = uit->second;
    if (static_cast<int>(u.state) >= static_cast<int>(CongestionState::CONGESTED)) {
      s.upstream_bottleneck = u.ref.id;
      break;
    }
  }
  s.summary = "domain " + std::to_string(d.ref.id.value()) + " state=" +
             std::to_string(static_cast<int>(a.state));
  return s;
}

const CongestionEpisode* CongestionFabric::current_episode(CongestionDomainId domain) const {
  std::shared_lock lk(impl_->mtx);
  auto it = impl_->domains_.find(domain);
  if (it == impl_->domains_.end()) return nullptr;
  return &it->second.current_episode;
}

std::vector<CongestionEpisode> CongestionFabric::episode_history(CongestionDomainId domain) const {
  std::shared_lock lk(impl_->mtx);
  std::vector<CongestionEpisode> out;
  auto it = impl_->domains_.find(domain);
  if (it != impl_->domains_.end()) out = it->second.episodes;
  return out;
}

void CongestionFabric::recover_dynamic_evidence() {
  std::unique_lock lk(impl_->mtx);
  impl_->recovered_ = true;
  for (auto& [id, d] : impl_->domains_) {
    (void)id;
    if (d.state == CongestionState::UNKNOWN) continue;
    d.state = CongestionState::REVALIDATION_REQUIRED;
    d.state_provenance = MeasurementProvenance::UNKNOWN;
    d.queue.generation = d.queue.generation.next();
  }
  for (auto& [id, f] : impl_->flows_) {
    (void)id;
    if (!f.is_terminal() && !flow_is_stale_or_superseded(f.state))
      f.state = FlowState::REVALIDATION_REQUIRED;
  }
}

// ---------------------------------------------------------------------------
// Persistence save / load
// ---------------------------------------------------------------------------
bool CongestionFabric::save(const std::string& path) const {
  using namespace persist;
  // Consistent snapshot under shared lock. This is a read-only operation and
  // must not race with concurrent mutations; shared_lock allows concurrent
  // readers while blocking writers.
  std::shared_lock lk(impl_->mtx);
  Writer payload;
  payload.u64(impl_->epoch_.value());
  // worker boots
  payload.u32(static_cast<std::uint32_t>(impl_->worker_boot_.size()));
  for (const auto& [w, b] : impl_->worker_boot_) {
    payload.u64(w.value());
    payload.u64(b.value());
  }
  payload.u32(static_cast<std::uint32_t>(impl_->source_boot_.size()));
  for (const auto& [s, b] : impl_->source_boot_) {
    payload.u64(s.value());
    payload.u64(b.value());
  }
  // domains
  payload.u32(static_cast<std::uint32_t>(impl_->domains_.size()));
  for (const auto& [id, d] : impl_->domains_) {
    wr(payload, id);
    wr_domain(payload, d);
  }
  // flows
  payload.u32(static_cast<std::uint32_t>(impl_->flows_.size()));
  for (const auto& [id, f] : impl_->flows_) {
    wr(payload, id);
    wr_flow(payload, f);
  }
  payload.u64(impl_->next_domain_);
  payload.u64(impl_->next_flow_);
  payload.u64(impl_->next_event_);
  payload.u64(impl_->next_policy_);

  // Envelope: magic + version + payload_len + len_crc + payload + payload_crc.
  Writer env;
  env.raw(reinterpret_cast<const std::uint8_t*>(kMagic), 8);
  env.u32(kVersion);
  env.u64(static_cast<std::uint64_t>(payload.size()));
  std::uint64_t len = static_cast<std::uint64_t>(payload.size());
  std::uint32_t len_crc = crc32(reinterpret_cast<const std::uint8_t*>(&len), sizeof(len));
  env.u32(len_crc);
  env.bytes(payload.data());
  env.u32(payload.crc());

  std::ofstream f(path, std::ios::binary);
  if (!f) return false;
  f.write(reinterpret_cast<const char*>(env.data().data()),
          static_cast<std::streamsize>(env.size()));
  return f.good();
}

bool CongestionFabric::load(const std::string& path) {
  using namespace persist;
  std::ifstream infile(path, std::ios::binary);
  if (!infile) return false;
  infile.seekg(0, std::ios::end);
  std::streamoff filelen = infile.tellg();
  infile.seekg(0, std::ios::beg);
  if (filelen <= 0 || static_cast<std::uint64_t>(filelen) > (1ULL << 30)) return false;
  std::vector<std::uint8_t> buf(static_cast<std::size_t>(filelen));
  if (!infile.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(filelen)))
    return false;

  try {
    Reader env(buf);
    std::uint8_t magic[8];
    env.raw(magic, 8);
    if (std::memcmp(magic, kMagic, 8) != 0) throw CorruptError("bad magic");
    std::uint32_t version = env.u32();
    if (version != kVersion) throw CorruptError("unsupported version");
    std::uint64_t plen = env.u64();
    if (plen > kMaxPayloadBytes) throw CorruptError("payload too large");
    std::uint64_t rawlen = plen;
    std::uint32_t len_crc = env.u32();
    if (crc32(reinterpret_cast<const std::uint8_t*>(&rawlen), sizeof(rawlen)) != len_crc)
      throw CorruptError("length checksum mismatch");
    if (env.remaining() < plen) throw CorruptError("truncated payload");
    std::vector<std::uint8_t> payload_bytes = env.bytes(static_cast<std::size_t>(plen));
    Reader payload(payload_bytes);
    std::uint32_t stored_crc = env.u32();
    if (env.remaining() != 0) throw CorruptError("trailing garbage");

    // Envelope-level integrity is checked over the payload bytes.
    const std::uint8_t* pstart = buf.data() + (env.offset() - plen - 4);
    if (crc32(pstart, static_cast<std::size_t>(plen)) != stored_crc)
      throw CorruptError("payload checksum mismatch");

    CoordinatorEpoch epoch(payload.u64());
    if (!epoch.is_valid()) throw CorruptError("invalid epoch");

    std::unordered_map<WorkerId, WorkerBootId> worker_boot;
    std::uint32_t wcount = payload.u32();
    if (wcount > kMaxCollectionCount) throw CorruptError("worker count too large");
    for (std::uint32_t i = 0; i < wcount; ++i) {
      WorkerId w(payload.u64());
      WorkerBootId b(payload.u64());
      if (!w.is_valid() || !b.is_valid()) throw CorruptError("invalid worker boot");
      if (!worker_boot.emplace(w, b).second) throw CorruptError("duplicate worker id");
    }
    std::unordered_map<SourceId, SourceBootId> source_boot;
    std::uint32_t scount = payload.u32();
    if (scount > kMaxCollectionCount) throw CorruptError("source count too large");
    for (std::uint32_t i = 0; i < scount; ++i) {
      SourceId s(payload.u64());
      SourceBootId b(payload.u64());
      if (!s.is_valid() || !b.is_valid()) throw CorruptError("invalid source boot");
      if (!source_boot.emplace(s, b).second) throw CorruptError("duplicate source id");
    }

    std::unordered_map<CongestionDomainId, CongestionDomain> domains;
    std::uint32_t dcount = payload.u32();
    if (dcount > kMaxCollectionCount) throw CorruptError("domain count too large");
    std::unordered_map<CongestionDomainId, std::uint64_t> domain_gen;
    for (std::uint32_t i = 0; i < dcount; ++i) {
      CongestionDomainId id(payload.u64());
      if (!id.is_valid()) throw CorruptError("invalid domain id");
      CongestionDomain d;
      rd(payload, d.ref.id);
      rd(payload, d.ref.generation);
      if (d.ref.id != id) throw CorruptError("domain id mismatch");
      d.kind = rd_domain_kind(payload);
      Rate theo, conf, meas, res;
      rd_rate(payload, theo);
      rd_rate(payload, conf);
      rd_rate(payload, meas);
      rd_rate(payload, res);
      d.capacity = CapacityProfile().set_theoretical(theo).set_configured(conf).set_measured_service(meas).set_reserved(res);
      CapacityGeneration cg;
      rd(payload, cg);
      d.capacity.set_generation(cg);
      rd(payload, d.queue.generation);
      std::uint8_t st = payload.u8();
      if (!congestion_state_valid(st)) throw CorruptError("invalid congestion state");
      d.state = static_cast<CongestionState>(st);
      std::uint8_t sp = payload.u8();
      if (!provenance_valid(sp)) throw CorruptError("invalid state provenance");
      d.state_provenance = static_cast<MeasurementProvenance>(sp);
      rd(payload, d.backpressure_generation);
      rd(payload, d.recovery_generation);
      FairnessConfig df;
      std::uint8_t dm = payload.u8();
      if (!fairness_model_valid(dm)) throw CorruptError("invalid fairness model");
      df.model = static_cast<FairnessModel>(dm);
      df.weight = payload.f64();
      df.minimum_guarantee_bps = payload.f64();
      df.cap_bps = payload.f64();
      df.latency_critical_protected = payload.u8() != 0;
      rd(payload, df.policy_generation);
      if (!df.valid()) throw CorruptError("invalid fairness config");
      d.default_fairness = df;
      double shape_rate = payload.f64();
      double shape_cap = payload.f64();
      if (!std::isfinite(shape_rate) || shape_rate < 0.0 || !std::isfinite(shape_cap) || shape_cap < 0.0)
        throw CorruptError("invalid shaping config");
      d.shaping = TokenBucket(shape_rate, shape_cap);
      d.offered_bps = payload.f64();
      d.offered_provenance = rd_prov(payload);
      d.admitted_bps = payload.f64();
      d.admitted_provenance = rd_prov(payload);
      d.serviced_bps = payload.f64();
      d.serviced_provenance = rd_prov(payload);
      d.utilisation = payload.f64();
      d.utilisation_provenance = rd_prov(payload);
      d.effective_bps = payload.f64();
      d.effective_provenance = rd_prov(payload);
      d.residual_bps = payload.f64();
      d.residual_provenance = rd_prov(payload);
      // flow refs
      std::uint64_t nfc = payload.bounded_count(kMaxCollectionCount);
      for (std::uint64_t k = 0; k < nfc; ++k) {
        EntityRef<Flow> fr;
        rd(payload, fr.id);
        rd(payload, fr.generation);
        d.flows.push_back(fr);
      }
      std::uint64_t nep = payload.bounded_count(kMaxCollectionCount);
      for (std::uint64_t k = 0; k < nep; ++k) {
        CongestionEpisode e;
        rd(payload, e.event_id);
        rd(payload, e.generation);
        rd(payload, e.domain.id);
        rd(payload, e.domain.generation);
        std::uint8_t ps = payload.u8();
        if (!congestion_state_valid(ps)) throw CorruptError("invalid episode peak state");
        e.peak_state = static_cast<CongestionState>(ps);
        e.start_ms = payload.u64();
        e.peak_ms = payload.u64();
        e.recovery_start_ms = payload.u64();
        e.end_ms = payload.u64();
        e.trigger = payload.str();
        e.peak_backlog_bytes = payload.f64();
        e.service_deficit_bytes = payload.f64();
        std::uint64_t naf = payload.bounded_count(kMaxCollectionCount);
        for (std::uint64_t a = 0; a < naf; ++a) {
          FlowId fid(payload.u64());
          e.affected_flows.push_back(fid);
        }
        rd(payload, e.bottleneck.id);
        rd(payload, e.bottleneck.generation);
        std::uint8_t oc = payload.u8();
        if (!congestion_state_valid(oc)) throw CorruptError("invalid episode outcome");
        e.outcome = static_cast<CongestionState>(oc);
        e.open = payload.u8() != 0;
        if (!e.valid()) throw CorruptError("invalid episode");
        d.episodes.push_back(e);
      }
      CongestionEpisode cur;
      rd(payload, cur.event_id);
      rd(payload, cur.generation);
      rd(payload, cur.domain.id);
      rd(payload, cur.domain.generation);
      std::uint8_t ps = payload.u8();
      if (!congestion_state_valid(ps)) throw CorruptError("invalid current episode peak");
      cur.peak_state = static_cast<CongestionState>(ps);
      cur.start_ms = payload.u64();
      cur.peak_ms = payload.u64();
      cur.recovery_start_ms = payload.u64();
      cur.end_ms = payload.u64();
      cur.trigger = payload.str();
      cur.peak_backlog_bytes = payload.f64();
      cur.service_deficit_bytes = payload.f64();
      std::uint64_t naf = payload.bounded_count(kMaxCollectionCount);
      for (std::uint64_t a = 0; a < naf; ++a) {
        FlowId fid(payload.u64());
        cur.affected_flows.push_back(fid);
      }
      rd(payload, cur.bottleneck.id);
      rd(payload, cur.bottleneck.generation);
      std::uint8_t oc = payload.u8();
      if (!congestion_state_valid(oc)) throw CorruptError("invalid current episode outcome");
      cur.outcome = static_cast<CongestionState>(oc);
      cur.open = payload.u8() != 0;
      d.current_episode = cur;

      if (!d.capacity.valid()) throw CorruptError("invalid capacity");
      auto agen = domain_gen.find(id);
      if (agen != domain_gen.end() && d.ref.generation.value() <= agen->second)
        throw CorruptError("domain generation regression");
      domain_gen[id] = d.ref.generation.value();
      if (!domains.emplace(id, std::move(d)).second) throw CorruptError("duplicate domain id");
    }

    std::unordered_map<FlowId, Flow> flows;
    std::uint32_t fcount = payload.u32();
    if (fcount > kMaxCollectionCount) throw CorruptError("flow count too large");
    std::unordered_map<FlowId, std::uint64_t> flow_gen;
    for (std::uint32_t i = 0; i < fcount; ++i) {
      FlowId id(payload.u64());
      if (!id.is_valid()) throw CorruptError("invalid flow id");
      Flow f = rd_flow(payload);
      if (f.ref.id != id) throw CorruptError("flow id mismatch");
      if (!flow_state_valid(static_cast<std::uint8_t>(f.state))) throw CorruptError("invalid flow state");
      if (!traffic_class_valid(static_cast<std::uint8_t>(f.traffic_class)))
        throw CorruptError("invalid traffic class");
      if (!f.accounting_valid()) throw CorruptError("invalid flow accounting");
      if (!f.fairness.valid()) throw CorruptError("invalid flow fairness");
      auto agen = flow_gen.find(id);
      if (agen != flow_gen.end() && f.ref.generation.value() <= agen->second)
        throw CorruptError("flow generation regression");
      flow_gen[id] = f.ref.generation.value();
      if (!flows.emplace(id, std::move(f)).second) throw CorruptError("duplicate flow id");
    }

    std::uint64_t nd = payload.u64();
    std::uint64_t nf = payload.u64();
    std::uint64_t ne = payload.u64();
    std::uint64_t np = payload.u64();
    if (payload.remaining() != 0) throw CorruptError("trailing garbage");

    // Commit only after full validation.
    {
      std::unique_lock lk(impl_->mtx);
      impl_->epoch_ = epoch;
      impl_->worker_boot_ = std::move(worker_boot);
      impl_->source_boot_ = std::move(source_boot);
      impl_->domains_ = std::move(domains);
      impl_->flows_ = std::move(flows);
      impl_->flow_history_.clear();
      for (const auto& [id, f] : impl_->flows_) impl_->flow_history_.push_back(id);
      impl_->next_domain_ = nd;
      impl_->next_flow_ = nf;
      impl_->next_event_ = ne;
      impl_->next_policy_ = np;
      // Conservative recovery: durable history/config survives, but dynamic
      // facts (live queue depth, current rates, active flow progress) must NOT
      // remain authoritative after a coordinator restart. Mark them for
      // revalidation; fresh evidence must be republished before current
      // congestion is asserted.
      for (auto& [did, d] : impl_->domains_) {
        (void)did;
        if (d.state != CongestionState::UNKNOWN) {
          d.state = CongestionState::REVALIDATION_REQUIRED;
          d.state_provenance = MeasurementProvenance::UNKNOWN;
          d.queue.generation = d.queue.generation.next();
        }
      }
      for (auto& [fid, f] : impl_->flows_) {
        (void)fid;
        if (!f.is_terminal() && !flow_is_stale_or_superseded(f.state))
          f.state = FlowState::REVALIDATION_REQUIRED;
      }
    }
    return true;
  } catch (const CorruptError&) {
    return false;
  }
}

}  // namespace congfabric
