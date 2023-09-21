#include "ycsbgen.hpp"
#include <iostream>
#include <thread>

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: <program> <workload_file>" << std::endl;
    return -1;
  }
  YCSBGen::YCSBGeneratorOptions options = YCSBGen::YCSBGeneratorOptions::ReadFromFile(argv[1]);
  std::cerr << options.ToString() << std::endl;
  YCSBGen::YCSBGenerator gen(options);
  std::vector<std::thread> pool;
  for(int i=0;i<1;i++) {
    pool.emplace_back([&, i]() {
      std::mt19937_64 rndgen(i + options.base_seed);
      std::ofstream out("out"+std::to_string(i));
      while(!gen.IsEOF()) {
        auto op = gen.GetNextOp(rndgen);
        out << int(op.type) << ": " << op.key << ", " << std::string(op.value.data(), op.value.size()) << "\n"; 
      }
    });
  }
  for (auto& a : pool) a.join();
}