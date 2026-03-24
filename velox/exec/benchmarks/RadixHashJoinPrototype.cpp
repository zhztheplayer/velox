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

#include <folly/init/Init.h>
#include <fmt/core.h>

#include "velox/exec/HashTable.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

class RadixHashJoinPrototype : public facebook::velox::test::VectorTestBase {
 public:
  void run() {
    auto buildType =
        ROW({"k1", "k2", "payload"}, {BIGINT(), VARCHAR(), BIGINT()});
    auto probeType = ROW({"k1", "k2"}, {BIGINT(), VARCHAR()});

    auto topTable = makeBuildTable();
    topTable->enableRadixPartitioning(3);
    copyRows(makeBuildBatch(0, 900), topTable.get());
    topTable->forceGenericHashMode(BaseHashTable::kNoSpillInputStartPartitionBit);

    std::vector<std::unique_ptr<BaseHashTable>> otherTables;
    auto otherTable = makeBuildTable();
    copyRows(makeBuildBatch(900, 900), otherTable.get());
    otherTable->forceGenericHashMode(
        BaseHashTable::kNoSpillInputStartPartitionBit);
    otherTables.push_back(std::move(otherTable));

    topTable->prepareJoinTable(
        std::move(otherTables),
        BaseHashTable::kNoSpillInputStartPartitionBit,
        1'000'000);

    auto probe = std::make_shared<RowVector>(
        pool_.get(),
        probeType,
        nullptr,
        1'800,
        std::vector<VectorPtr>{
            this->makeFlatVector<int64_t>(
                1'800,
                [](auto row) { return row; }),
            this->makeFlatVector<std::string>(
                1'800,
                [](auto row) { return std::to_string(row); })});

    auto probeHashers = createProbeHashers();
    HashLookup lookup(probeHashers, pool_.get());
    SelectivityVector rows(probe->size());
    rows.setAll();
    topTable->prepareForJoinProbe(lookup, probe, rows, true);
    lookup.hits.resize(probe->size());
    topTable->joinProbe(lookup);

    int64_t hits{0};
    for (auto row : lookup.rows) {
      hits += lookup.hits[row] != nullptr;
    }

    fmt::print(
        "radix_bits={} partitions={} hash_mode={} build_rows={} probe_rows={} hits={}\n",
        topTable->radixPartitionBits(),
        1U << topTable->radixPartitionBits(),
        BaseHashTable::modeString(topTable->hashMode()),
        topTable->numDistinct(),
        lookup.rows.size(),
        hits);
  }

 private:
  std::unique_ptr<HashTable<true>> makeBuildTable() {
    return HashTable<true>::createForJoin(
        createProbeHashers(),
        {BIGINT()},
        true,
        false,
        0,
        pool_.get());
  }

  std::vector<std::unique_ptr<VectorHasher>> createProbeHashers() {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.push_back(std::make_unique<VectorHasher>(BIGINT(), 0));
    hashers.push_back(std::make_unique<VectorHasher>(VARCHAR(), 1));
    return hashers;
  }

  RowVectorPtr makeBuildBatch(int64_t start, int64_t size) {
    return this->makeRowVector(
        {this->makeFlatVector<int64_t>(
             size, [start](auto row) { return start + row; }),
         this->makeFlatVector<std::string>(
             size,
             [start](auto row) {
               return std::to_string(start + row);
             }),
         this->makeFlatVector<int64_t>(
             size, [start](auto row) { return (start + row) * 10; })});
  }

  void copyRows(const RowVectorPtr& batch, BaseHashTable* table) {
    auto& hashers = table->hashers();
    const auto numKeys = hashers.size();
    SelectivityVector rows(batch->size());
    rows.setAll();
    raw_vector<uint64_t> dummy(batch->size(), pool_.get());
    DecodedVector payload;

    for (int32_t i = 0; i < batch->childrenSize(); ++i) {
      if (i < numKeys) {
        hashers[i]->decode(*batch->childAt(i), rows);
        if (table->hashMode() != BaseHashTable::HashMode::kHash &&
            hashers[i]->mayUseValueIds()) {
          hashers[i]->computeValueIds(rows, dummy);
        }
      }
    }
    payload.decode(*batch->childAt(2), rows);

    auto* rowContainer = table->rows();
    rows.applyToSelected([&](auto row) {
      char* newRow = rowContainer->newRow();
      if (auto nextOffset = rowContainer->nextOffset()) {
        *reinterpret_cast<char**>(newRow + nextOffset) = nullptr;
      }
      for (int32_t i = 0; i < numKeys; ++i) {
        rowContainer->store(hashers[i]->decodedVector(), row, newRow, i);
      }
      rowContainer->store(payload, row, newRow, numKeys);
    });
  }

  memory::MemoryManager* const memoryManager_{
      memory::MemoryManager::getInstance()};
  std::shared_ptr<memory::MemoryPool> pool_{
      memoryManager_->addLeafPool("radix_hash_join_prototype")};
};

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv, false);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  RadixHashJoinPrototype prototype;
  prototype.run();
  return 0;
}
