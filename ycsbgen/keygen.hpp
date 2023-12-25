#pragma once

#include "zipf.hpp"
#include "hash.hpp"
#include <random>
#include <atomic>

namespace YCSBGen {

class KeyGenerator {
 public:
  /* Generate a random key from the distribution */
  virtual uint64_t GenKey(std::mt19937_64&) = 0;

};

class ZipfianGenerator : public KeyGenerator {
  zipf_distribution<> gen_;
 
 public:
  /* zipfian constant is in [0, 1]. it is uniform when constant = 0. */
  ZipfianGenerator(uint64_t n, double constant)
   : gen_(n, constant) {}

  uint64_t GenKey(std::mt19937_64& rndgen) override {
    return gen_(rndgen);
  }
};

class ScrambledZipfianGenerator : public KeyGenerator {
  uint64_t l_, r_;
  IntHasher hasher_;
  ZipfianGenerator gen_;

 public:
  ScrambledZipfianGenerator(uint64_t l, uint64_t r, double constant) 
    : l_(l), r_(r), gen_(r - l, constant) {}
  
  uint64_t GenKey(std::mt19937_64& rndgen) override {
    auto ret = gen_.GenKey(rndgen);
    return l_ + hasher_(ret) % (r_ - l_);
  }

};

class UniformGenerator : public KeyGenerator {
  uint64_t l_, r_;

 public:
  UniformGenerator(uint64_t l, uint64_t r) : l_(l), r_(r) {}

  uint64_t GenKey(std::mt19937_64& rndgen) override {
    std::uniform_int_distribution<> dis(l_, r_ - 1);
    return dis(rndgen);
  }

};

// Generate hotspot distribution in range [l, r).
class HotspotGenerator : public KeyGenerator {
  uint64_t l_, hotspot_r_, r_, offset_;
  double hotspot_opn_fraction_;

 public:
  HotspotGenerator(uint64_t l, uint64_t r, uint64_t offset, double hotspot_set_fraction, double hotspot_opn_fraction)
    : l_(l), hotspot_r_(l + hotspot_set_fraction * (r - l)), r_(r), offset_(offset), hotspot_opn_fraction_(hotspot_opn_fraction) {}

  uint64_t GenKey(std::mt19937_64& rndgen) override {
    std::uniform_real_distribution<> dis(0, 1);
    uint64_t ret = 0;
    if (dis(rndgen) <= hotspot_opn_fraction_) {
      std::uniform_int_distribution<> dis_key(l_, hotspot_r_ - 1);
      ret = dis_key(rndgen) + offset_;
    } else {
      std::uniform_int_distribution<> dis_key(hotspot_r_, r_ - 1);
      ret = dis_key(rndgen) + offset_;
    }
    if (ret >= r_) ret = (ret - l_) % (r_ - l_) + l_;
    return ret;
  }

};

// Generate hotspot distribution in range [l, r).
// Two phases. Each phase has a hotspot distribution of different offsets.
class HotspotShiftingGenerator : public KeyGenerator {
  HotspotGenerator phase1_gen_;
  HotspotGenerator phase2_gen_;
  uint64_t phase1_op_;
  std::atomic<uint64_t> count_{0};

 public:
  struct PhaseConfig {
    uint64_t offset;
    double hotspot_set_fraction;
    double hotspot_opn_fraction;
  };
  HotspotShiftingGenerator(uint64_t l, uint64_t r, PhaseConfig phase1,
                           PhaseConfig phase2, uint64_t phase1_op)
      : phase1_gen_(l, r, phase1.offset, phase1.hotspot_set_fraction,
                    phase1.hotspot_opn_fraction),
        phase2_gen_(l, r, phase2.offset, phase2.hotspot_set_fraction,
                    phase2.hotspot_opn_fraction),
        phase1_op_(phase1_op) {}

  uint64_t GenKey(std::mt19937_64& rndgen) override {
    if (count_.load(std::memory_order_relaxed) <= phase1_op_) {
      if (count_.fetch_add(1, std::memory_order_relaxed) <= phase1_op_) {
        return phase1_gen_.GenKey(rndgen);  
      }
    }
    return phase2_gen_.GenKey(rndgen);
  }

};

class LatestGenerator : public KeyGenerator {
  std::atomic<uint64_t>& now_keys_;
  zipf_distribution<> gen_;
  uint64_t n_;

 public:
  LatestGenerator(std::atomic<uint64_t>& now_keys) : now_keys_(now_keys), gen_(100), n_(100) {}

  uint64_t GenKey(std::mt19937_64& rndgen) override {
    auto nw_n = now_keys_.load(std::memory_order_relaxed);
    if (n_ != nw_n) {
      gen_.set_n(nw_n);
      n_ = nw_n;
    }
    return n_ - 1 - gen_(rndgen);
  }
  
};

}