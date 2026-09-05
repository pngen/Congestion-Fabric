#pragma once
// Congestion Fabric -- bounded local reference shaping.
// This is governed *software* shaping inside the reference/local congestion
// domain implementation. It does not claim to control NIC/GPU driver hardware.
// Measured physical resource behavior is tracked separately.
#include <algorithm>
#include <cmath>
#include <cstdint>

namespace congfabric {

// Deterministic virtual-time token bucket. Uses caller-supplied monotonically
// increasing timestamps (seconds). It never creates bandwidth; it only bounds
// how quickly bytes are released.
class TokenBucket {
 public:
  TokenBucket() = default;
  TokenBucket(double rate_bps, double burst_bytes)
      : rate_(rate_bps), capacity_(burst_bytes), tokens_(burst_bytes) {}

  void set_rate(double rate_bps) { rate_ = rate_bps; }
  void set_burst(double burst_bytes) {
    capacity_ = burst_bytes;
    tokens_ = std::min(tokens_, capacity_);
  }
  double rate() const { return rate_; }
  double capacity() const { return capacity_; }

  void refill(double now_s) {
    double elapsed = now_s - last_refill_s_;
    if (elapsed > 0.0) {
      tokens_ = std::min(capacity_, tokens_ + elapsed * rate_);
      last_refill_s_ = now_s;
    }
  }

  bool try_consume(double bytes) {
    if (bytes < 0.0) return false;
    if (tokens_ >= bytes) { tokens_ -= bytes; return true; }
    return false;
  }

  double available() const { return tokens_; }
  bool valid() const {
    return std::isfinite(rate_) && rate_ >= 0.0 && std::isfinite(capacity_) &&
           capacity_ >= 0.0 && std::isfinite(tokens_) && tokens_ >= 0.0;
  }

 private:
  double rate_{0.0};
  double capacity_{0.0};
  double tokens_{0.0};
  double last_refill_s_{0.0};
};

// Weighted byte budget: distributes a per-window budget among participants
// proportional to a weight, optionally protecting a latency-critical share.
class WeightedByteBudget {
 public:
  WeightedByteBudget(double total_budget, double protect_bps = 0.0)
      : total_(total_budget), protect_(protect_bps) {}

  double share_for(double weight, double total_weight) const {
    if (total_weight <= 0.0 || weight < 0.0) return 0.0;
    double pool = total_ - protect_;
    if (pool < 0.0) pool = 0.0;
    return pool * (weight / total_weight);
  }

 private:
  double total_{0.0};
  double protect_{0.0};
};

}  // namespace congfabric
