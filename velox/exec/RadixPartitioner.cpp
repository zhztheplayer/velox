#include "velox/exec/RadixPartitioner.h"

#include "velox/common/testutil/TestValue.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {
namespace {

RowVectorPtr wrapChildren(
    memory::MemoryPool* pool,
    const RowVectorPtr& input,
    const BufferPtr& indices,
    vector_size_t size) {
  auto result = std::make_shared<RowVector>(
      pool,
      input->type(),
      nullptr,
      size,
      std::vector<VectorPtr>(input->childrenSize()));
  for (auto i = 0; i < input->childrenSize(); ++i) {
    result->childAt(i) =
        BaseVector::wrapInDictionary(nullptr, indices, size, input->childAt(i));
  }
  result->updateContainsLazyNotLoaded();
  return result;
}

RowVectorPtr copyBatches(
    memory::MemoryPool* pool,
    const std::vector<RowVectorPtr>& inputs,
    vector_size_t size) {
  VELOX_CHECK(!inputs.empty());
  auto result = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create<RowVector>(inputs.front()->type(), 0, pool));
  result->resize(size);

  vector_size_t targetOffset = 0;
  std::vector<BaseVector::CopyRange> ranges;
  for (const auto& input : inputs) {
    ranges.resize(input->size());
    for (auto i = 0; i < input->size(); ++i) {
      ranges[i] = BaseVector::CopyRange{i, targetOffset + i, 1};
    }
    result->copyRanges(
        input.get(),
        folly::Range<const BaseVector::CopyRange*>(
            ranges.data(), ranges.data() + ranges.size()));
    targetOffset += input->size();
  }
  result->updateContainsLazyNotLoaded();
  return result;
}

class BufferedRadixPartitioner final : public RadixPartitioner {
 public:
  struct PartitionState {
    vector_size_t bufferedRows{0};
    std::deque<RowVectorPtr> queue;
  };

  BufferedRadixPartitioner(
      BaseHashTable& table,
      vector_size_t numMaxBufferedRows,
      vector_size_t minOutputBatchSize,
      memory::MemoryPool* pool)
      : table_(table),
        numMaxBufferedRows_(numMaxBufferedRows),
        minOutputBatchSize_(minOutputBatchSize),
        pool_(pool),
        lookup_(std::make_unique<HashLookup>(table.hashers(), pool)),
        partitions_(1u << table.radixPartitionBits()) {
    VELOX_CHECK_GT(numMaxBufferedRows_, 0);
    VELOX_CHECK_GT(minOutputBatchSize_, 0);
    VELOX_CHECK(table_.isRadixPartitioned());
  }

  void addInput(RowVectorPtr input) override {
    VELOX_CHECK_NOT_NULL(input);
    if (input->size() == 0) {
      return;
    }

    auto partitionRows = partitionInput(input);
    for (auto partition = 0; partition < numPartitions(); ++partition) {
      auto& rows = partitionRows[partition];
      if (rows.empty()) {
        continue;
      }
      enqueuePartitionVector(makePartitionVector(input, rows), partition);
    }
  }

  RowVectorPtr getOutput() override {
    if (!hasReadyOutput()) {
      return nullptr;
    }

    if (currentDrainingPartition_ < 0) {
      int32_t largestPartition = findLargestPartition();
      auto& largestPartitionState = partitions_[largestPartition];
      VELOX_CHECK_GT(largestPartitionState.bufferedRows, 0);
      currentDrainingPartition_ = largestPartition;
    }
    auto& [bufferedRows, queue] = partitions_[currentDrainingPartition_];

    if (bufferedRows == 0) {
      VELOX_CHECK(queue.empty());
      return nullptr;
    }
    std::vector<RowVectorPtr> outputs;
    vector_size_t outputSize = 0;
    while (!queue.empty() && outputSize < minOutputBatchSize_) {
      outputSize += queue.front()->size();
      outputs.push_back(std::move(queue.front()));
      queue.pop_front();
    }
    auto output =
        outputs.size() == 1 ? std::move(outputs.front())
                            : copyBatches(pool_, outputs, outputSize);
    bufferedRows -= outputSize;
    totalBufferedRows_ -= outputSize;
    if (queue.empty()) {
      VELOX_CHECK_EQ(bufferedRows, 0);
      currentDrainingPartition_ = -1;
    }
    common::testutil::TestValue::adjust(
        "facebook::velox::exec::RadixPartitioner::collect", this);
    return output;
  }

  void noMoreInput() override {
    noMoreInput_ = true;
  }

  bool hasReadyOutput() const override {
    if (noMoreInput_) {
      return totalBufferedRows_ > 0;
    }
    if (currentDrainingPartition_ >= 0) {
      return true;
    }
    if (totalBufferedRows_ >= numMaxBufferedRows_) {
      return true;
    }
    return false;
  }

  bool hasBufferedData() const override {
    return totalBufferedRows_ > 0;
  }

 private:
  RowVectorPtr makePartitionVector(
      const RowVectorPtr& input,
      const std::vector<vector_size_t>& rows) {
    auto indices = allocateIndices(rows.size(), pool_);
    auto* rawIndices = indices->asMutable<vector_size_t>();
    std::copy(rows.begin(), rows.end(), rawIndices);
    return wrapChildren(pool_, input, indices, rows.size());
  }

  int32_t numPartitions() const {
    return static_cast<int32_t>(partitions_.size());
  }

  void enqueuePartitionVector(RowVectorPtr output, int32_t partition) {
    VELOX_CHECK_NOT_NULL(output);
    auto& partitionState = partitions_[partition];
    partitionState.queue.push_back(std::move(output));
    partitionState.bufferedRows += partitionState.queue.back()->size();
    totalBufferedRows_ += partitionState.queue.back()->size();
  }

 private:
  std::vector<std::vector<vector_size_t>> partitionInput(
      const RowVectorPtr& input) const {
    SelectivityVector rows(input->size());
    auto& hashers = lookup_->hashers;
    lookup_->reset(rows.end());

    for (auto& hasher : hashers) {
      auto key = input->childAt(hasher->channel())->loadedVector();
      hasher->decode(*key, rows);
    }

    const auto mode = table_.hashMode();
    for (auto i = 0; i < hashers.size(); ++i) {
      auto& hasher = hashers[i];
      if (mode != BaseHashTable::HashMode::kHash) {
        auto& key = input->childAt(hasher->channel());
        hasher->lookupValueIds(*key, rows, lookup_->scratchMemory, lookup_->hashes);
      } else {
        hasher->hash(rows, i > 0, lookup_->hashes);
      }
    }

    lookup_->rows.resize(input->size());
    std::iota(lookup_->rows.begin(), lookup_->rows.end(), 0);

    std::vector<vector_size_t> counts(numPartitions(), 0);
    for (auto row : lookup_->rows) {
      ++counts[table_.getRadixPartition(lookup_->hashes[row])];
    }

    std::vector<std::vector<vector_size_t>> partitionRows(numPartitions());
    for (auto partition = 0; partition < numPartitions(); ++partition) {
      partitionRows[partition].reserve(counts[partition]);
    }
    for (auto row : lookup_->rows) {
      partitionRows[table_.getRadixPartition(lookup_->hashes[row])].push_back(
          row);
    }
    return partitionRows;
  }

  int32_t findLargestPartition() const {
    int32_t largestPartition = 0;
    vector_size_t largestBufferedRows = 0;
    for (auto partition = 0; partition < numPartitions(); ++partition) {
      const auto bufferedRows = partitions_[partition].bufferedRows;
      if (bufferedRows > largestBufferedRows) {
        largestPartition = partition;
        largestBufferedRows = bufferedRows;
      }
    }
    return largestPartition;
  }

 private:
  BaseHashTable& table_;
  const vector_size_t numMaxBufferedRows_;
  const vector_size_t minOutputBatchSize_;
  memory::MemoryPool* const pool_;
  std::unique_ptr<HashLookup> lookup_;

  std::vector<PartitionState> partitions_;
  vector_size_t totalBufferedRows_{0};
  bool noMoreInput_{false};
  int32_t currentDrainingPartition_{-1};
};

} // namespace

std::unique_ptr<RadixPartitioner> RadixPartitioner::createBuffered(
    BaseHashTable& table,
    vector_size_t numMaxBufferedRows,
    vector_size_t minOutputBatchSize,
    memory::MemoryPool* pool) {
  return std::make_unique<BufferedRadixPartitioner>(
      table, numMaxBufferedRows, minOutputBatchSize, pool);
}

} // namespace facebook::velox::exec
