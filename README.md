# Congestion Fabric

**Congestion Fabric is an open-source, vendor-neutral C++20 runtime for detecting, modeling, explaining, and governing congestion across accelerator interconnects, PCIe, NICs, storage, host memory, and collective traffic in heterogeneous AI infrastructure.**

**It answers one systems question:**

**Where is infrastructure traffic congested now, which flows and resource generations are causing it, how much useful capacity remains, and what bounded action should callers take before queues, latency, or throughput collapse?**

Congestion Fabric exists because nominal bandwidth is not usable bandwidth. A path may advertise high theoretical throughput while actual work is constrained by queue buildup, competing transfers, PCIe oversubscription, accelerator copy-engine contention, host-memory pressure, pinned-memory bottlenecks, NIC saturation, storage bandwidth saturation, collective overlap, shared-link contention, topology bottlenecks, flow burstiness, head-of-line blocking, unfair traffic, stale capacity assumptions, asymmetric send/receive pressure, transient recovery traffic, checkpoint traffic, model/state movement, inference communication, and background replication.

Congestion Fabric makes that state explicit and distinguishes **capacity → demand → offered load → admitted load → active load → queueing → service → congestion → backpressure → recovery**. It never treats congestion as a single utilization percentage.

## Systems boundary

Congestion Fabric is intentionally narrow. It does not absorb the responsibilities of adjacent systems:

- **Resource Broker** owns scarce-resource arbitration and claims.
- **Reservation Fabric** owns advance commitments and reservations.
- **Capacity Fabric** owns general capacity forecasting and future capacity.
- **Fragmentation Governor** owns stranded-capacity and placement-fragmentation remediation.
- **Topology Fabric**, **PCIe Fabric**, and the NUMA / runtime / fleet layers own physical topology and capability facts.
- **Collective Fabric** owns collective execution semantics.
- **Fabric Scheduler** decides topology-aware workload placement.
- **Communication Planner** computes point-to-point, collective, multicast, staged, and hierarchical communication plans.
- **Collective Scheduler** decides when competing collectives execute.
- **Interference Observatory** later measures broader cross-workload interference across cache, memory, PCIe, collectives, NUMA, storage, and other shared resources.
- **Contention Governor** later resolves cross-workload policy / SLO / value conflicts.

**Congestion Fabric** governs live shared-path congestion: congestion-domain identity, link/path/resource traffic observations, offered-load and serviced-load accounting, queue/backlog state, saturation, pressure and congestion classification, congestion epochs/generations, flow contribution attribution, bottleneck identification, available effective bandwidth, congestion propagation, deterministic congestion explanations, bounded backpressure/admission/shaping intents, fairness envelopes local to the congestion domain, congestion recovery state, event history, congestion-aware query APIs, and conservative distributed recovery with proof that stale traffic evidence cannot remain authoritative.

It may recommend or expose bounded local actions. It never becomes a global scheduler or a cross-workload policy optimizer.

## Defining thesis

**Congestion is not utilization. It is the loss of usable service caused when offered traffic, queueing, topology, and competing flows exceed the effective service capacity of a shared infrastructure path.**

High utilization may be healthy; low utilization may coexist with severe head-of-line blocking. Advertised bandwidth is not serviced bandwidth. Submitted bytes are not completed bytes. A transient queue is not necessarily persistent congestion, and a dropped process does not prove its in-flight traffic completed.

## What is implemented

- **Strongly typed identities and generations** for congestion domains, flows, links, paths, resources, devices, nodes, NICs, storage endpoints, memory domains, collectives, workloads, executions, workers, sources, coordinator epochs, measurements, queues, backpressure, recovery, and authority. A stale FlowGeneration cannot mutate current accounting; a stale WorkerBootId cannot publish live evidence; a stale LinkGeneration cannot satisfy a current congestion evaluation.
- **Explicit congestion domains** including PCIE_LINK, PCIE_ROOT_COMPLEX, GPU_COPY_ENGINE, GPU_HOST_TRANSFER, GPU_PEER_PATH, NVLINK_CLASS_PATH, NIC_TX, NIC_RX, NETWORK_PATH, STORAGE_READ/WRITE/QUEUE, HOST_MEMORY_BANDWIDTH, PINNED_HOST_MEMORY_PATH, NUMA_MEMORY_PATH, COLLECTIVE_PATH/GROUP, SHARED_TRANSFER_POOL, COMPOSITE_PATH, and UNKNOWN.
- **First-class flows** with a guarded lifecycle (DECLARED → VALIDATING → ADMITTED → QUEUED → ACTIVE → BACKPRESSURED/THROTTLED/DRAINING → COMPLETED/CANCELLED/FAILED, plus STALE/SUPERSEDED/REVALIDATION_REQUIRED/RETIRED), checked byte accounting (expected/submitted/admitted/in-flight/completed/failed/cancelled), and explicit authority (worker + source boot).
- **Measurement provenance** distinguishing MEASURED, REPORTED, DERIVED, ESTIMATED, SYNTHETIC, RECONSTRUCTED, and UNKNOWN.
- **Congestion states** (UNKNOWN, IDLE, HEALTHY, BUSY, APPROACHING_SATURATION, SATURATED, QUEUEING, CONGESTED, SEVERELY_CONGESTED, BACKPRESSURED, RECOVERING, STALE, REVALIDATION_REQUIRED) derived from evidence, not a mechanical threshold.
- **Congestion epochs** (first-class generation-bound events), **effective-bandwidth** distinction (theoretical/configured/measured-service/effective/residual/reserved/congested), **bottleneck identification**, **directed congestion propagation**, **local fairness** (equal/weighted/minimum-guarantee/capped/protected), **bounded backpressure intents** (ALLOW, ADMIT_LIMITED, THROTTLE, DEFER, PAUSE_NEW_TRAFFIC, DRAIN, REDUCE_BURST, REQUEST_REROUTE/REPLAN/RESCHEDULE/PREEMPTION, REVALIDATE, NO_ACTION, UNKNOWN), and **bounded local software shaping** (token bucket / weighted byte budget).
- **Versioned binary persistence** with CRC-32 integrity checking, bounded counts, strict corruption rejection (bad magic, unsupported version, truncation, checksum mismatch, trailing garbage, invalid enums, generation regression, impossible accounting), and **conservative recovery** — durable history/config survives, dynamic evidence becomes STALE / REVALIDATION_REQUIRED and must be republished.
- **A real OS-process multiprocess reference deployment** over framed, checksummed TCP: a coordinator plus worker A/B processes, real loopback framing, HELLO/REGISTER, fresh WorkerBootIds, partial-read/write correctness, real worker kill/restart, stale-boot replay rejection, and coordinator restart recovery.

## Repository layout

- `include/congestion_fabric/` — public headers.
- `src/` — engine, persistence, protocol, coordinator.
- `tools/` — `cfcoordinator`, `cfworker`, and `cfcli` (inspection).
- `tests/` — unit, property, concurrency, adversarial, physical-traffic, multiprocess, restart, CUDA-death suites.
- `examples/` — runnable examples.
- `benchmarks/` — scale benchmark.
- `cuda/` — RTX 5090 (sm_120) CUDA transfer proofs.
- `cmake/` — package config. The installed target is `CongestionFabric::CongestionFabric`.

## Building

```
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build
```

CUDA proofs (RTX 5090, sm_120) are enabled with `-DCONGFABRIC_BUILD_CUDA_PROOFS=ON`.

## License

Apache License 2.0. Copyright 2026 Summon Software Labs. No telemetry transmission.
