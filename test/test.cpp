#include "ycsbgen.hpp"
#include <iostream>

int main() {
  YCSBGen::YCSBGeneratorOptions options;
  options.record_counts = 1e6;
  options.operation_counts = 4e7;
  options.read_proportions = 1;
  options.insert_proportions = 0;
  YCSBGen::YCSBGenerator gen(options);
  while(!gen.IsEOF()) {
    auto op = gen.GetNextOp();
    // std::cout << op.key << ", " << std::string(op.value.data(), op.value.size()) << "\n"; 
  }
}