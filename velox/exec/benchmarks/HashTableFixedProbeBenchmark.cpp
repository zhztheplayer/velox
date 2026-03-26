/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <iostream>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <fmt/format.h>

#include "velox/common/base/SelectivityInfo.h"
#include "velox/exec/RadixPartitioner.h"
#include "velox/exec/HashTable.h"
#include "velox/vector/tests/utils/VectorMaker.h"

DEFINE_int64(
    build_size,
    0,
    "Custom number of build rows. If zero, runs built-in cases.");
DEFINE_int64(
    probe_size,
    1 << 30,
    "Number of probe rows for the custom case.");
DEFINE_int64(
    batch_size,
    4096,
    "Maximum number of probe rows materialized at once.");
DEFINE_int64(
    num_radix_bits,
    0,
    "Number of radix bits to build. Use 0 to disable radix.");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {

class EagerPassThroughRadixPartitioner final : public RadixPartitioner {
 public:
  void addInput(RowVectorPtr input) override {
    VELOX_CHECK_NOT_NULL(input);
    if (input->size() == 0) {
      return;
    }
    queue_.push_back(std::move(input));
  }

  RowVectorPtr collect() override {
    if (queue_.empty()) {
      return nullptr;
    }
    auto output = std::move(queue_.front());
    queue_.pop_front();
    return output;
  }

  void forceCollectAll() override {}

  bool hasReadyOutput() const override {
    return !queue_.empty();
  }

  bool hasBufferedData() const override {
    return !queue_.empty();
  }

 private:
  std::deque<RowVectorPtr> queue_;
};

struct FixedProbeParams {
  std::string title;
  int64_t buildSize;
  int64_t probeSize;
  uint8_t numRadixBits;

  FixedProbeParams(
      std::string title,
      int64_t buildSize,
      int64_t probeSize,
      uint8_t numRadixBits)
      : title(std::move(title)),
        buildSize(buildSize),
        probeSize(probeSize),
        numRadixBits(numRadixBits) {
    VELOX_CHECK_GE(buildSize, 1, "buildSize must be positive");
    VELOX_CHECK_GE(probeSize, 1, "probeSize must be positive");
    VELOX_CHECK_GE(
        probeSize,
        buildSize,
        "probeSize must be larger than buildSize for the repeated all-hit benchmark");
  }

  std::string toString() const {
    return fmt::format(
        "{}: BuildRows={} ProbeRows={} RadixBits={}",
        title,
        buildSize,
        probeSize,
        numRadixBits);
  }
};

struct FixedProbeResult {
  FixedProbeParams params{"default", 1, 2, 0};
  int64_t numHashed{0};
  int64_t numProbed{0};
  int64_t numHit{0};
  float hashClocks{0};
  float probeClocks{0};
  int64_t numDistinct{0};
  int64_t sizeBytes{0};
  int64_t bucketBytes{0};
  int64_t rowBytes{0};
  BaseHashTable::HashMode mode{BaseHashTable::HashMode::kHash};

  std::string toString() const {
    std::stringstream out;
    out << params.toString() << '\n'
        << fmt::format(
               "Hashed: {} Probed: {} Hit: {} Mode: {} Hash time/row {} probe time/row {} bucketBytes {} rowBytes {}",
               numHashed,
               numProbed,
               numHit,
               BaseHashTable::modeString(mode),
               hashClocks,
               probeClocks,
               bucketBytes,
               rowBytes)
        << '\n'
        << " numDistinct=" << numDistinct << " sizeBytes=" << sizeBytes;
    return out.str();
  }
};

class FixedProbeBenchmark {
 public:
  void makeData(const FixedProbeParams& params) {
    params_ = params;
    table_.reset();
    probePartitioner_.reset();
    buildRows_.clear();
    expectedHits_.clear();

    const bool radixEnabled = params_.numRadixBits > 0;

    auto buildKeys = vectorMaker_.flatVector<int64_t>(
        params_.buildSize, [&](vector_size_t row) { return int64_t{row}; });
    build_ = vectorMaker_.rowVector({"k1"}, {buildKeys});

    const auto batchSize = std::min<int64_t>(
        params_.probeSize, std::max<int64_t>(1, FLAGS_batch_size));
    probeKeys_ = vectorMaker_.flatVector<int64_t>(batchSize);
    probe_ = vectorMaker_.rowVector({"k1"}, {probeKeys_});

    std::vector<std::unique_ptr<VectorHasher>> keyHashers;
    keyHashers.push_back(std::make_unique<VectorHasher>(BIGINT(), 0));
    std::vector<TypePtr> dependentTypes;
    table_ = HashTable<true>::createForJoin(
        std::move(keyHashers),
        dependentTypes,
        true,
        false,
        1'000,
        pool_.get());

    populateRows(*build_, table_.get());
    std::vector<std::unique_ptr<BaseHashTable>> otherTables;
    table_->prepareJoinTable(
        std::move(otherTables),
        BaseHashTable::kNoSpillInputStartPartitionBit,
        1'000'000,
        false,
        nullptr);
    if (radixEnabled) {
      table_->buildRadixPartitions(params_.numRadixBits);
      auto numAccumulatedRows = params_.buildSize / (1ULL << params_.numRadixBits) * 10;
      probePartitioner_ = RadixPartitioner::createCopied(*table_, numAccumulatedRows, pool_.get());
    } else {
      probePartitioner_ = std::make_unique<EagerPassThroughRadixPartitioner>();
    }
    buildExpectedHits();
  }

  FixedProbeResult run() {
    FixedProbeResult result;
    result.params = params_;

    auto lookup = std::make_unique<HashLookup>(table_->hashers(), pool_.get());
    SelectivityInfo hashTime;
    SelectivityInfo probeTime;
    int64_t numHit = 0;
    int64_t numHashed = 0;
    int64_t numProbed = 0;

    for (int64_t offset = 0; offset < params_.probeSize;
         offset += probeKeys_->size()) {
      const auto batchSize =
          std::min<int64_t>(probeKeys_->size(), params_.probeSize - offset);
      fillProbeBatch(offset, batchSize);
      probePartitioner_->addInput(probe_);

      while (auto partitionedInput = probePartitioner_->collect()) {
        const auto inputSize = partitionedInput->size();
        SelectivityVector rows(inputSize);

        {
          SelectivityTimer timer(hashTime, 0);
          table_->prepareForJoinProbe(*lookup, partitionedInput, rows, true);
        }
        numHashed += inputSize;

        {
          SelectivityTimer timer(probeTime, 0);
          table_->joinProbe(*lookup);
        }
        numProbed += inputSize;

        DecodedVector decodedKeys;
        decodedKeys.decode(*partitionedInput->childAt(0), rows);
        for (auto row = 0; row < inputSize; ++row) {
          const auto key = decodedKeys.valueAt<int64_t>(row);
          numHit += lookup->hits[row] != nullptr;
          VELOX_CHECK_GE(key, 0);
          VELOX_CHECK_LT(key, params_.buildSize);
          VELOX_CHECK_EQ(expectedHits_[key], lookup->hits[row]);
        }
      }
    }
    VELOX_CHECK_EQ(numHit, params_.probeSize);

    result.numHashed = numHashed;
    result.numProbed = numProbed;
    result.numHit = numHit;
    result.hashClocks = hashTime.timeToDropValue() / numHashed;
    result.probeClocks = probeTime.timeToDropValue() / numProbed;
    result.numDistinct = table_->numDistinct();
    result.rowBytes = table_->rows()->allocatedBytes();
    result.bucketBytes = table_->allocatedBytes() - result.rowBytes;
    result.sizeBytes = table_->allocatedBytes();
    result.mode = table_->hashMode();

    return result;
  }

 private:
  void populateRows(const RowVector& input, BaseHashTable* table) {
    SelectivityVector rows(input.size());
    std::vector<DecodedVector> decoded(input.childrenSize());
    for (auto i = 0; i < input.childrenSize(); ++i) {
      decoded[i].decode(*input.childAt(i), rows);
    }

    auto rowContainer = table->rows();
    auto nextOffset = rowContainer->nextOffset();
    buildRows_.resize(input.size());
    for (auto row = 0; row < input.size(); ++row) {
      char* newRow = rowContainer->newRow();
      buildRows_[row] = newRow;
      if (nextOffset) {
        *reinterpret_cast<char**>(newRow + nextOffset) = nullptr;
      }
      for (auto i = 0; i < input.childrenSize(); ++i) {
        rowContainer->store(decoded[i], row, newRow, i);
      }
    }
  }

  void fillProbeBatch(int64_t offset, int64_t batchSize) {
    probeKeys_->resize(batchSize);
    probe_->resize(batchSize);
    for (vector_size_t row = 0; row < batchSize; ++row) {
      const auto probeRow = offset + row;
      probeKeys_->set(row, probeRow % params_.buildSize);
    }
  }

  void buildExpectedHits() {
    expectedHits_.assign(params_.buildSize, nullptr);

    auto rows = raw_vector<char*>(pool_.get());
    rows.resize(table_->rows()->numRows());
    RowContainerIterator iterator;
    vector_size_t numRows = 0;
    while (auto numListed =
               table_->rows()->listRows(&iterator, 1024, rows.data() + numRows)) {
      numRows += numListed;
    }
    VELOX_CHECK_EQ(numRows, params_.buildSize);

    auto keysVector = BaseVector::create(BIGINT(), numRows, pool_.get());
    RowContainer::extractColumn(
        rows.data(),
        numRows,
        table_->rows()->columnAt(0),
        table_->rows()->columnHasNulls(0),
        keysVector);
    auto flatKeys = keysVector->asFlatVector<int64_t>();

    for (vector_size_t row = 0; row < numRows; ++row) {
      const auto key = flatKeys->valueAt(row);
      VELOX_CHECK_GE(key, 0);
      VELOX_CHECK_LT(key, params_.buildSize);
      expectedHits_[key] = rows[row];
    }
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  VectorMaker vectorMaker_{pool_.get()};
  RowVectorPtr build_;
  RowVectorPtr probe_;
  FlatVectorPtr<int64_t> probeKeys_;
  std::vector<char*> buildRows_;
  std::vector<char*> expectedHits_;
  std::unique_ptr<HashTable<true>> table_;
  std::unique_ptr<RadixPartitioner> probePartitioner_;
  FixedProbeParams params_{"default", 1, 2, 0};
};

void combineResults(
    std::vector<FixedProbeResult>& results,
    FixedProbeResult result) {
  if (!results.empty() && results.back().params.title == result.params.title) {
    return;
  }
  results.push_back(std::move(result));
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};

  memory::MemoryManager::Options options;
  options.useMmapAllocator = true;
  options.allocatorCapacity = 32UL << 30;
  options.useMmapArena = true;
  options.mmapArenaCapacityRatio = 1;
  memory::MemoryManager::initialize(options);

  FixedProbeBenchmark benchmark;
  std::vector<FixedProbeResult> results;

  std::vector<FixedProbeParams> params = {
      FixedProbeParams("Probe1GTable128B", 1 << 7, 1 << 30, 0),
      FixedProbeParams("Probe1GTable256B", 1 << 8, 1 << 30, 0),
      FixedProbeParams("Probe1GTable512B", 1 << 9, 1 << 30, 0),
      FixedProbeParams("Probe1GTable1K", 1 << 10, 1 << 30, 0),
      FixedProbeParams("Probe1GTable2K", 1 << 11, 1 << 30, 0),
      FixedProbeParams("Probe1GTable4K", 1 << 12, 1 << 30, 0),
      FixedProbeParams("Probe1GTable8K", 1 << 13, 1 << 30, 0),
      FixedProbeParams("Probe1GTable16K", 1 << 14, 1 << 30, 0),
      FixedProbeParams("Probe1GTable32K", 1 << 15, 1 << 30, 0),
      FixedProbeParams("Probe1GTable64K", 1 << 16, 1 << 30, 0),
      FixedProbeParams("Probe1GTable128K", 1 << 17, 1 << 30, 0),
      FixedProbeParams("Probe1GTable256K", 1 << 18, 1 << 30, 0),
      FixedProbeParams("Probe1GTable512K", 1 << 19, 1 << 30, 0),
      FixedProbeParams("Probe1GTable1M", 1 << 20, 1 << 30, 0),
      FixedProbeParams("Probe1GTable2M", 1 << 21, 1 << 30, 0),
      FixedProbeParams("Probe1GTable4M", 1 << 22, 1 << 30, 0),
      FixedProbeParams("Probe1GTable8M", 1 << 23, 1 << 30, 0),
      FixedProbeParams("Probe1GTable16M", 1 << 24, 1 << 30, 0),
      FixedProbeParams("Probe1GTable32M", 1 << 25, 1 << 30, 0),
      FixedProbeParams("Probe1GTable64M", 1 << 26, 1 << 30, 0),
      FixedProbeParams("Probe1GTable128M", 1 << 27, 1 << 30, 0),
      FixedProbeParams("Probe1GTable256M", 1 << 28, 1 << 30, 0),
      FixedProbeParams("Probe1GTable512M", 1 << 29, 1 << 30, 0),
  };
  if (FLAGS_build_size != 0) {
    VELOX_CHECK_GE(FLAGS_num_radix_bits, 0, "num_radix_bits must be >= 0");
    VELOX_CHECK_LE(FLAGS_num_radix_bits, std::numeric_limits<uint8_t>::max(), "num_radix_bits must be <= 255");
    params = {FixedProbeParams(
        "Custom",
        FLAGS_build_size,
        FLAGS_probe_size,
        static_cast<uint8_t>(FLAGS_num_radix_bits))};
  }

  for (const auto& param : params) {
    folly::addBenchmark(
        __FILE__,
        param.title,
        [param, &benchmark, &results, once = std::make_shared<std::once_flag>()]() {
          std::call_once(*once, [&] {
            folly::BenchmarkSuspender suspender;
            benchmark.makeData(param);
          });
          combineResults(results, benchmark.run());
          return 1;
        });
  }

  folly::runBenchmarks();
  std::cout << "*** Results:" << std::endl;
  for (const auto& result : results) {
    std::cout << result.toString() << std::endl;
  }
  return 0;
}
