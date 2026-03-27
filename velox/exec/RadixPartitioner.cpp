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
  for (const auto& input : inputs) {
    BaseVector::CopyRange range{0, targetOffset, input->size()};
    result->copyRanges(input.get(), folly::Range(&range, 1));
    targetOffset += input->size();
  }
  result->updateContainsLazyNotLoaded();
  return result;
}

class BufferedRadixPartitioner final : public RadixPartitioner {
 public:
  BufferedRadixPartitioner(
      std::shared_ptr<BaseHashTable> table,
      vector_size_t numMaxBufferedRows,
      vector_size_t minOutputBatchSize,
      memory::MemoryPool* pool)
      : table_(std::move(table)),
        numMaxBufferedRows_(numMaxBufferedRows),
        minOutputBatchSize_(minOutputBatchSize),
        pool_(pool),
        lookup_(std::make_unique<HashLookup>(table_->hashers(), pool)),
        partitionRows_(1u << table_->radixPartitionBits()),
        bufferedRowsPerPartition_(1u << table_->radixPartitionBits(), 0),
        partitionQueues_(1u << table_->radixPartitionBits()),
        partitionReady_(1u << table_->radixPartitionBits(), false) {
    VELOX_CHECK_GT(numMaxBufferedRows_, 0);
    VELOX_CHECK_GT(minOutputBatchSize_, 0);
    VELOX_CHECK(table_->isRadixPartitioned());
  }

  void addInput(RowVectorPtr input) override {
    VELOX_CHECK_NOT_NULL(input);
    if (input->size() == 0) {
      return;
    }
    partitionInput(input);
    for (auto partition = 0; partition < numPartitions(); ++partition) {
      auto& rows = partitionRows_[partition];
      if (rows.empty()) {
        continue;
      }
      partitionQueues_[partition].push_back(makePartitionVector(input, rows));
      bufferedRowsPerPartition_[partition] += rows.size();
      if (bufferedRowsPerPartition_[partition] >= numMaxBufferedRows_) {
        markReady(partition);
      }
    }
  }

  RowVectorPtr getOutput() override {
    if (readyPartitions_.empty()) {
      return nullptr;
    }

    const auto partition = readyPartitions_.front();
    auto& queue = partitionQueues_[partition];
    VELOX_CHECK(!queue.empty());
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
    bufferedRowsPerPartition_[partition] -= outputSize;
    if (queue.empty()) {
      VELOX_CHECK_EQ(bufferedRowsPerPartition_[partition], 0);
      readyPartitions_.pop_front();
      partitionReady_[partition] = false;
    }
    common::testutil::TestValue::adjust(
        "facebook::velox::exec::RadixPartitioner::collect", this);
    return output;
  }

  void noMoreInput() override {
    noMoreInput_ = true;
    for (auto partition = 0; partition < numPartitions(); ++partition) {
      if (!partitionQueues_[partition].empty()) {
        markReady(partition);
      }
    }
  }

  bool hasReadyOutput() const override {
    return !readyPartitions_.empty();
  }

  bool hasBufferedData() const override {
    return std::any_of(
        bufferedRowsPerPartition_.begin(),
        bufferedRowsPerPartition_.end(),
        [](auto rows) { return rows > 0; });
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
    return static_cast<int32_t>(bufferedRowsPerPartition_.size());
  }

 private:
  void partitionInput(const RowVectorPtr& input) {
    SelectivityVector rows(input->size());
    auto& hashers = lookup_->hashers;
    lookup_->reset(rows.end());

    for (auto& hasher : hashers) {
      auto key = input->childAt(hasher->channel())->loadedVector();
      hasher->decode(*key, rows);
    }

    const auto mode = table_->hashMode();
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

    auto rowPartition = [&](vector_size_t row) {
      // lookupValueIds() deselects rows with unmappable values in array and
      // normalized-key modes. Keep these rows in the buffered probe stream by
      // routing them to a stable fallback partition instead of reading an
      // uninitialized value-id/hash slot.
      if (mode != BaseHashTable::HashMode::kHash && !rows.isValid(row)) {
        return uint32_t{0};
      }

      const auto partition = table_->getRadixPartition(lookup_->hashes[row]);
      VELOX_DCHECK_LT(
          partition,
          numPartitions(),
          "Invalid radix partition {} for hash {} in hash mode {} with {} partitions, radixBits={}, capacity={}",
          partition,
          lookup_->hashes[row],
          BaseHashTable::modeString(table_->hashMode()),
          numPartitions(),
          table_->radixPartitionBits(),
          table_->capacity());
      return partition;
    };

    for (auto& partitionRows : partitionRows_) {
      partitionRows.clear();
    }
    for (auto row : lookup_->rows) {
      partitionRows_[rowPartition(row)].push_back(row);
    }
  }

  void markReady(int32_t partition) {
    if (partitionReady_[partition]) {
      return;
    }
    partitionReady_[partition] = true;
    readyPartitions_.push_back(partition);
  }

 private:
  const std::shared_ptr<BaseHashTable> table_;
  const vector_size_t numMaxBufferedRows_;
  const vector_size_t minOutputBatchSize_;
  memory::MemoryPool* const pool_;
  std::unique_ptr<HashLookup> lookup_;
  std::vector<std::vector<vector_size_t>> partitionRows_;
  std::vector<vector_size_t> bufferedRowsPerPartition_;
  std::vector<std::deque<RowVectorPtr>> partitionQueues_;
  std::vector<bool> partitionReady_;
  std::deque<int32_t> readyPartitions_;
  bool noMoreInput_{false};
};

} // namespace

std::unique_ptr<RadixPartitioner> RadixPartitioner::createBuffered(
    std::shared_ptr<BaseHashTable> table,
    vector_size_t numMaxBufferedRows,
    vector_size_t minOutputBatchSize,
    memory::MemoryPool* pool) {
  return std::make_unique<BufferedRadixPartitioner>(
      std::move(table), numMaxBufferedRows, minOutputBatchSize, pool);
}

std::unique_ptr<RadixPartitioner> RadixPartitioner::createBuffered(
    BaseHashTable& table,
    vector_size_t numMaxBufferedRows,
    vector_size_t minOutputBatchSize,
    memory::MemoryPool* pool) {
  return createBuffered(
      std::shared_ptr<BaseHashTable>(&table, [](BaseHashTable*) {}),
      numMaxBufferedRows,
      minOutputBatchSize,
      pool);
}

} // namespace facebook::velox::exec
