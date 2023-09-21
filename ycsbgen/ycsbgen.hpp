#pragma once

#include <string>
#include <random>
#include <cstring>
#include <memory>
#include "zipf.hpp"
#include "hash.hpp"
#include "keygen.hpp"

namespace YCSBGen {

struct YCSBGeneratorOptions {
  uint64_t record_counts{10};
  uint64_t operation_counts{10};
  double read_proportions{1};
  double insert_proportions{0};
  double update_proportions{0};
  double rmw_proportions{0};
  double zipfian_constant{0.99};
  double hotspot_opn_fraction{0.1};
  double hotspot_set_fraction{0.1};
  size_t value_len{1000};
  size_t seed{0x202309202027};
  std::string request_distribution{"zipfian"};

};

enum class OpType {
  INSERT,
  READ,
  UPDATE,
  RMW,
};


struct Operation {
  OpType type;
  std::string key;
  std::vector<char> value;

  Operation() {}

  Operation(OpType _type, const std::string& _key, const std::vector<char>& _value) :
    type(_type), key(_key), value(_value) {}

  Operation(OpType _type, std::string&& _key, std::vector<char>&& _value) :
    type(_type), key(std::move(_key)), value(std::move(_value)) {}
};

class YCSBGenerator {
  YCSBGeneratorOptions options_;
  std::mt19937_64 rndgen_;
  uint64_t now_keys_{0};
  uint64_t now_ops_{0};
  IntHasher key_hasher_;
  
  std::unique_ptr<KeyGenerator> key_generator_;

 public:
  YCSBGenerator(const YCSBGeneratorOptions& options) 
    : options_(options), rndgen_(options.seed) {
    uint64_t estimate_key_counts = options.record_counts + 2 * options.operation_counts * options.insert_proportions;
    if (options.request_distribution == "zipfian") {
      key_generator_ = std::unique_ptr<KeyGenerator>(new ScrambledZipfianGenerator(0, estimate_key_counts, options.zipfian_constant, options.seed));
    } else if (options.request_distribution == "uniform") {
      key_generator_ = std::unique_ptr<KeyGenerator>(new UniformGenerator(0, estimate_key_counts, options.seed));
    } else if (options.request_distribution == "hotspot") {
      key_generator_ = std::unique_ptr<KeyGenerator>(new HotspotGenerator(0, estimate_key_counts, options.hotspot_set_fraction, options.hotspot_opn_fraction, options.seed));
    }
  }

  Operation GetNextOp() {
    if (now_ops_ >= options_.record_counts) {
      now_ops_ += 1;
      std::uniform_real_distribution<> dis(0, 1);
      double x = dis(rndgen_);
      if (x <= options_.read_proportions) {
        return GenRead();
      } else if (x <= options_.read_proportions + options_.insert_proportions) {
        return GenInsert();
      } else if (x <= options_.read_proportions + options_.insert_proportions + options_.update_proportions) {
        return GenUpdate();
      } else {
        return GenRMW();
      }
    } else {
      now_ops_ += 1;
      return GenInsert();
    }
  }

  bool IsEOF() const {
    return now_ops_ >= options_.record_counts + options_.operation_counts;
  }

 private:
  Operation GenInsert() {
    Operation ret;
    ret.type = OpType::INSERT;
    ret.key = BuildKeyName(now_keys_++);
    ret.value = GenNewValue(ret.key);
    return ret;
  }

  Operation GenRead() {
    Operation ret;
    ret.type = OpType::READ;
    ret.key = ChooseKey();
    return ret;
  }

  Operation GenUpdate() {
    Operation ret;
    ret.type = OpType::UPDATE;
    ret.key = ChooseKey();
    ret.value = GenNewValue(ret.key);
    return ret;
  }

  Operation GenRMW() {
    Operation ret;
    ret.type = OpType::RMW;
    ret.key = ChooseKey();
    ret.value = GenNewValue(ret.key);
    return ret;
  }

  std::string BuildKeyName(uint64_t key) {
    return "user" + std::to_string(key_hasher_(key));
  }

  std::string ChooseKey() {
    while (true) {
      auto ret = key_generator_->GenKey();
      if (ret < now_keys_) {
        return BuildKeyName(ret);
      }
    }
  }

  std::vector<char> GenNewValue(const std::string& key) {
    std::vector<char> v(options_.value_len);
    std::memcpy(v.data(), key.data(), std::min(v.size(), key.size()));
    return v;
  }

};

}