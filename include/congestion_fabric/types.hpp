#pragma once
// Congestion Fabric -- concrete identity/generation domain aliases.
#include "congestion_fabric/identities.hpp"
#include <cstdint>

namespace congfabric {

// A distinct entity tag for every authority domain. Used to keep each domain's
// Id/Generation a genuinely different C++ type.
#define CONGFABRIC_ID_GEN(Name)            \
  struct Name##Tag final {};                    \
  using Name##Id = Id<Name##Tag>;               \
  using Name##Generation = Generation<Name##Tag>;

#define CONGFABRIC_GEN_ONLY(Name)          \
  struct Name##Tag final {};                    \
  using Name = Generation<Name##Tag>;

// ---- id + generation authority domains ------------------------------------
CONGFABRIC_ID_GEN(CongestionDomain)
CONGFABRIC_ID_GEN(CongestionSnapshot)
CONGFABRIC_ID_GEN(CongestionEvent)
CONGFABRIC_ID_GEN(CongestionPolicy)
CONGFABRIC_ID_GEN(Flow)
CONGFABRIC_ID_GEN(FlowGroup)
CONGFABRIC_ID_GEN(TrafficClass)
CONGFABRIC_ID_GEN(Link)
CONGFABRIC_ID_GEN(Path)
CONGFABRIC_ID_GEN(Resource)
CONGFABRIC_ID_GEN(Device)
CONGFABRIC_ID_GEN(Node)
CONGFABRIC_ID_GEN(Nic)
CONGFABRIC_ID_GEN(StorageEndpoint)
CONGFABRIC_ID_GEN(MemoryDomain)
CONGFABRIC_ID_GEN(Collective)
CONGFABRIC_ID_GEN(Workload)
CONGFABRIC_ID_GEN(Execution)

// ---- generation-only freshness domains -------------------------------------
CONGFABRIC_GEN_ONLY(TopologyGeneration)
CONGFABRIC_GEN_ONLY(CapabilityGeneration)
CONGFABRIC_GEN_ONLY(HealthGeneration)
CONGFABRIC_GEN_ONLY(CapacityGeneration)
CONGFABRIC_GEN_ONLY(ReservationGeneration)
CONGFABRIC_GEN_ONLY(MeasurementGeneration)
CONGFABRIC_GEN_ONLY(QueueGeneration)
CONGFABRIC_GEN_ONLY(BackpressureGeneration)
CONGFABRIC_GEN_ONLY(RecoveryGeneration)
CONGFABRIC_GEN_ONLY(AuthorityGeneration)
CONGFABRIC_GEN_ONLY(WorkerBootId)
CONGFABRIC_GEN_ONLY(SourceBootId)
CONGFABRIC_GEN_ONLY(CoordinatorEpoch)
CONGFABRIC_GEN_ONLY(ProtocolGeneration)

// ---- id-only domains --------------------------------------------------------
struct WorkerTag final {};
using WorkerId = Id<WorkerTag>;
struct SourceTag final {};
using SourceId = Id<SourceTag>;

#undef CONGFABRIC_ID_GEN
#undef CONGFABRIC_GEN_ONLY

// ---------------------------------------------------------------------------
// Entity type -> (Id, Generation) mapping. Each entity type is mapped to its
// own strongly-typed authority domain so a FlowRef, DomainRef, PathRef, ...
// are genuinely different types. Some entity structs are only ever referenced
// via EntityRef and are never instantiated; they remain forward-declared.
// ---------------------------------------------------------------------------
struct Flow;
struct CongestionDomain;
struct Path;
struct Workload;
struct Execution;
struct Resource;

#define CONGFABRIC_ENTITY_TRAITS(Name)                 \
  template <>                                          \
  struct EntityTraits<Name> {                          \
    using Id = Name##Id;                               \
    using Gen = Name##Generation;                      \
  };

CONGFABRIC_ENTITY_TRAITS(Flow)
CONGFABRIC_ENTITY_TRAITS(CongestionDomain)
CONGFABRIC_ENTITY_TRAITS(Path)
CONGFABRIC_ENTITY_TRAITS(Workload)
CONGFABRIC_ENTITY_TRAITS(Execution)
CONGFABRIC_ENTITY_TRAITS(Resource)

#undef CONGFABRIC_ENTITY_TRAITS

}  // namespace congfabric
