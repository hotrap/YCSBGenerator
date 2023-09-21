#pragma once

#include "zipf.hpp"
#include "hash.hpp"
#include <random>

namespace YCSBGen {

class KeyGenerator {
 public:
  /* Generate a random key from the distribution */
  virtual uint64_t GenKey() = 0;

};

class ZipfianGenerator : public KeyGenerator {
  zipf_distribution<> gen_;
  std::mt19937_64 rndgen_;
 
 public:
  /* zipfian constant is in [0, 1]. it is uniform when constant = 0. */
  ZipfianGenerator(uint64_t n, double constant, uint64_t seed)
   : gen_(n, constant), rndgen_(seed) {}

  uint64_t GenKey() override {
    return gen_(rndgen_);
  }
};

class ScrambledZipfianGenerator : public KeyGenerator {
  ZipfianGenerator gen_;
  uint64_t l_, r_;
  IntHasher hasher_;

 public:
  ScrambledZipfianGenerator(uint64_t l, uint64_t r, double constant, uint64_t seed) 
    : l_(l), r_(r), gen_(r - l, constant, seed) {}
  
  uint64_t GenKey() override {
    auto ret = gen_.GenKey();
    return l_ + hasher_(ret) % (r_ - l_);
  }

};

class UniformGenerator : public KeyGenerator {
  uint64_t l_, r_;
  std::mt19937_64 rndgen_;

 public:
  UniformGenerator(uint64_t l, uint64_t r, uint64_t seed) : l_(l), r_(r), rndgen_(seed) {}

  uint64_t GenKey() override {
    std::uniform_int_distribution<> dis(l_, r_ - 1);
    return dis(rndgen_);
  }

};

class HotspotGenerator : public KeyGenerator {
  uint64_t l_, hotspot_r_, r_;
  double hotspot_opn_fraction_;
  std::mt19937_64 rndgen_;

 public:
  HotspotGenerator(uint64_t l, uint64_t r, double hotspot_set_fraction, double hotspot_opn_fraction, uint64_t seed)
    : l_(l), hotspot_r_(l_ + hotspot_set_fraction * (r - l)), r_(r), hotspot_opn_fraction_(hotspot_opn_fraction), rndgen_(seed) {}

  uint64_t GenKey() override {
    std::uniform_real_distribution<> dis(0, 1);
    if (dis(rndgen_) <= hotspot_opn_fraction_) {
      std::uniform_int_distribution<> dis_key(l_, hotspot_r_);
      return dis_key(rndgen_); 
    } else {
      std::uniform_int_distribution<> dis_key(hotspot_r_, r_);
      return dis_key(rndgen_);
    }
  }

};

}