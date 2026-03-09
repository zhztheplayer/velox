#pragma once

#include <deque>

#include "velox/exec/HashTable.h"

namespace facebook::velox::exec {

// Reorganizes probe input into radix-partition-local RowVectors using the same
// partition function as the build-side radix-partitioned hash table.
class RadixPartitioner {
 public:
  virtual ~RadixPartitioner() = default;

  static std::unique_ptr<RadixPartitioner> createBuffered(
      BaseHashTable& table,
      vector_size_t numMaxBufferedRows,
      vector_size_t minOutputBatchSize,
      memory::MemoryPool* pool);

  virtual void addInput(RowVectorPtr input) = 0;

  virtual RowVectorPtr getOutput() = 0;

  virtual void noMoreInput() = 0;

  virtual bool hasReadyOutput() const = 0;

  virtual bool hasBufferedData() const = 0;
};

} // namespace facebook::velox::exec
