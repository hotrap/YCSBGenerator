#pragma once

#include <string>
#include <random>
#include <cstring>
#include <memory>
#include "zipf.hpp"
#include "hash.hpp"
#include "keygen.hpp"
#include <fstream>
#include <map>
#include <iostream>
#include <atomic>
#include <thread>

namespace YCSBGen {

struct YCSBGeneratorOptions {
  uint64_t record_count{10};
  uint64_t operation_count{10};
  double read_proportion{1};
  double insert_proportion{0};
  double update_proportion{0};
  double rmw_proportion{0};
  double zipfian_constant{0.99};
  double hotspot_opn_fraction{0.1};
  double hotspot_set_fraction{0.1};
  size_t value_len{1000};
  size_t base_seed{0x202309202027};
  std::string request_distribution{"zipfian"};
  uint64_t load_sleep{150}; // in seconds.

  static YCSBGeneratorOptions ReadFromFile(std::string filename) {
    std::ifstream in(filename);
    std::map<std::string, std::string> names;
    if (!in) {
      throw std::runtime_error("Invalid filename: " + filename);
    }

    while(!in.eof()) {
      std::string s;
      std::getline(in, s);
      uint32_t i = 0;
      while(i < s.size() && isspace(s[i])) i++;
      /* ignore the line if it begins with '#'. */
      if (i >= s.size() || s[i] == '#') continue;
      uint32_t name_i = i;
      /* find the first space or '='. Characters before it are the name. */
      while(i < s.size() && !isspace(s[i]) && s[i] != '=') i++;
      std::string name = s.substr(name_i, i - name_i);
      while(i < s.size() && s[i] != '=') i++;
      if (i >= s.size()) continue;
      i++;
      /* skip spaces after '='. */
      while(i < s.size() && isspace(s[i])) i++;
      uint32_t value_i = i;
      /* characters after '=' are the value. */
      while(i < s.size() && !isspace(s[i])) i++;
      names[name] = s.substr(value_i, i - value_i);
    }

    YCSBGeneratorOptions ret;
    if (names.count("recordcount")) ret.record_count = std::stoull(names["recordcount"]);
    if (names.count("operationcount")) ret.operation_count = std::stoull(names["operationcount"]);
    if (names.count("readproportion")) ret.read_proportion = std::stof(names["readproportion"]);
    if (names.count("insertproportion")) ret.insert_proportion = std::stof(names["insertproportion"]);
    if (names.count("updateproportion")) ret.update_proportion = std::stof(names["updateproportion"]);
    if (names.count("rmwproportion")) ret.rmw_proportion = std::stof(names["rmwproportion"]);
    if (names.count("zipfianconstant")) ret.zipfian_constant = std::stof(names["zipfianconstant"]);
    if (names.count("hotspotopnfraction")) ret.hotspot_opn_fraction = std::stof(names["hotspotopnfraction"]);
    if (names.count("hotspotdatafraction")) ret.hotspot_set_fraction = std::stof(names["hotspotdatafraction"]);
    if (names.count("valuelength")) ret.value_len = std::stoull(names["valuelength"]);
    else ret.value_len = (names.count("fieldcount") ? std::stoull(names["fieldcount"]) : 10) * (names.count("fieldlength") ? std::stoull(names["fieldlength"]) : 100);
    if (names.count("baseseed")) ret.base_seed = std::stoull(names["baseseed"]);
    if (names.count("requestdistribution")) ret.request_distribution = names["requestdistribution"];
    if (names.count("loadsleep")) ret.load_sleep = std::stoull(names["loadsleep"]);
    return ret;
  }

  std::string ToString() const {
    std::string ret;
    ret += "recordcount = " + std::to_string(record_count) + "\n";
    ret += "operationcount = " + std::to_string(operation_count) + "\n";
    ret += "readproportion = " + std::to_string(read_proportion) + "\n";
    ret += "insertproportion = " + std::to_string(insert_proportion) + "\n";
    ret += "updateproportion = " + std::to_string(update_proportion) + "\n";
    ret += "rmwproportion = " + std::to_string(rmw_proportion) + "\n";
    ret += "zipfianconstant = " + std::to_string(zipfian_constant) + "\n";
    ret += "hotspotopnfraction = " + std::to_string(hotspot_opn_fraction) + "\n";
    ret += "hotspotdatafraction = " + std::to_string(hotspot_set_fraction) + "\n";
    ret += "valuelength = " + std::to_string(value_len) + "\n";
    ret += "baseseed = " + std::to_string(base_seed) + "\n";
    ret += "requestdistribution = " + request_distribution + "\n";
    ret += "loadsleep = " + std::to_string(load_sleep) + "\n";
    return ret;
  }

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
  std::atomic<uint64_t> now_keys_{0};
  std::atomic<uint64_t> now_ops_{0};
  IntHasher key_hasher_;
  
  std::unique_ptr<KeyGenerator> key_generator_;

 public:
  YCSBGenerator(const YCSBGeneratorOptions& options) 
    : options_(options) {
    uint64_t estimate_key_count = options.record_count + 2 * options.operation_count * options.insert_proportion;
    if (options.request_distribution == "zipfian") {
      key_generator_ = std::unique_ptr<KeyGenerator>(new ScrambledZipfianGenerator(0, estimate_key_count, options.zipfian_constant));
    } else if (options.request_distribution == "uniform") {
      key_generator_ = std::unique_ptr<KeyGenerator>(new UniformGenerator(0, estimate_key_count));
    } else if (options.request_distribution == "hotspot") {
      key_generator_ = std::unique_ptr<KeyGenerator>(new HotspotGenerator(0, estimate_key_count, options.hotspot_set_fraction, options.hotspot_opn_fraction));
    } else if (options.request_distribution == "latest") {
      key_generator_ = std::unique_ptr<KeyGenerator>(new LatestGenerator(now_keys_));
    }
  }

  Operation GetNextOp(std::mt19937_64& rndgen) {
    if (now_ops_ == options_.record_count) {
      // Loading is complete. We will sleep for a few seconds (default: 150s) to wait for compaction to finish.
      std::this_thread::sleep_for(std::chrono::seconds(options_.load_sleep));
    }
    if (now_ops_ >= options_.record_count) {
      now_ops_ += 1;
      std::uniform_real_distribution<> dis(0, 1);
      double x = dis(rndgen);
      if (x <= options_.read_proportion) {
        return GenRead(rndgen);
      } else if (x <= options_.read_proportion + options_.insert_proportion) {
        return GenInsert();
      } else if (x <= options_.read_proportion + options_.insert_proportion + options_.update_proportion) {
        return GenUpdate(rndgen);
      } else {
        return GenRMW(rndgen);
      }
    } else {
      now_ops_ += 1;
      return GenInsert();
    }
  }

  bool IsEOF() const {
    return now_ops_ >= options_.record_count + options_.operation_count;
  }

 private:
  Operation GenInsert() {
    Operation ret;
    ret.type = OpType::INSERT;
    ret.key = BuildKeyName(now_keys_++);
    ret.value = GenNewValue(ret.key);
    return ret;
  }

  Operation GenRead(std::mt19937_64& rndgen) {
    Operation ret;
    ret.type = OpType::READ;
    ret.key = ChooseKey(rndgen);
    return ret;
  }

  Operation GenUpdate(std::mt19937_64& rndgen) {
    Operation ret;
    ret.type = OpType::UPDATE;
    ret.key = ChooseKey(rndgen);
    ret.value = GenNewValue(ret.key);
    return ret;
  }

  Operation GenRMW(std::mt19937_64& rndgen) {
    Operation ret;
    ret.type = OpType::RMW;
    ret.key = ChooseKey(rndgen);
    ret.value = GenNewValue(ret.key);
    return ret;
  }

  std::string BuildKeyName(uint64_t key) {
    return "user" + std::to_string(key_hasher_(key));
  }

  std::string ChooseKey(std::mt19937_64& rndgen) {
    while (true) {
      auto ret = key_generator_->GenKey(rndgen);
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