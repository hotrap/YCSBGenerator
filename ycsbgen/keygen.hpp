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

class HotspotGenerator : public KeyGenerator {
  uint64_t l_, hotspot_r_, r_;
  double hotspot_opn_fraction_;

 public:
  HotspotGenerator(uint64_t l, uint64_t r, double hotspot_set_fraction, double hotspot_opn_fraction)
    : l_(l), hotspot_r_(l + hotspot_set_fraction * (r - l)), r_(r), hotspot_opn_fraction_(hotspot_opn_fraction) {}

  uint64_t GenKey(std::mt19937_64& rndgen) override {
    std::uniform_real_distribution<> dis(0, 1);
    if (dis(rndgen) <= hotspot_opn_fraction_) {
      std::uniform_int_distribution<> dis_key(l_, hotspot_r_);
      return dis_key(rndgen); 
    } else {
      std::uniform_int_distribution<> dis_key(hotspot_r_, r_);
      return dis_key(rndgen);
    }
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