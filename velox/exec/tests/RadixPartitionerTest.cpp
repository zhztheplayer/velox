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

#include "velox/exec/HashTable.h"
#include "velox/exec/RadixPartitioner.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec::test {
namespace {

class RadixPartitionerTest : public testing::Test,
                             public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  std::unique_ptr<BaseHashTable> makeRadixTable() {
    std::vector<std::unique_ptr<VectorHasher>> keyHashers;
    keyHashers.emplace_back(std::make_unique<VectorHasher>(BIGINT(), 0));
    auto table = HashTable<true>::createForJoin(
        std::move(keyHashers), {}, true, false, 1'000, pool());

    auto batch = makeRowVector(std::vector<VectorPtr>{
        makeFlatVector<int64_t>(1 << 12, [](auto row) { return row; }),
    });
    copyToTable(batch, table.get());
    table->prepareJoinTable(
        {}, BaseHashTable::kNoSpillInputStartPartitionBit, 1'000'000);
    table->buildRadixPartitions(2);
    return table;
  }

  void copyToTable(const RowVectorPtr& batch, BaseHashTable* table) {
    std::vector<DecodedVector> decoded(batch->childrenSize());
    SelectivityVector allRows(batch->size());
    for (auto i = 0; i < batch->childrenSize(); ++i) {
      decoded[i].decode(*batch->childAt(i), allRows);
    }

    auto* rowContainer = table->rows();
    const auto nextOffset = rowContainer->nextOffset();
    for (auto row = 0; row < batch->size(); ++row) {
      auto* newRow = rowContainer->newRow();
      if (nextOffset > 0) {
        *reinterpret_cast<char**>(newRow + nextOffset) = nullptr;
      }
      for (auto column = 0; column < batch->childrenSize(); ++column) {
        rowContainer->store(decoded[column], row, newRow, column);
      }
    }
  }

  void assertPartitionedOutput(
      BaseHashTable& table,
      RadixPartitioner& partitioner,
      vector_size_t expectedRows,
      bool expectReadyBeforeNoMoreInput) {
    if (expectReadyBeforeNoMoreInput) {
      ASSERT_TRUE(partitioner.hasReadyOutput());
    } else {
      ASSERT_FALSE(partitioner.hasReadyOutput());
    }
    partitioner.noMoreInput();

    vector_size_t totalRows = 0;
    while (auto output = partitioner.getOutput()) {
      totalRows += output->size();

      HashLookup lookup(table.hashers(), pool());
      SelectivityVector rows(output->size());
      table.prepareForJoinProbe(lookup, output, rows, true);
      ASSERT_FALSE(lookup.rows.empty());
      const auto partition =
          table.getRadixPartition(lookup.hashes[lookup.rows[0]]);
      for (auto row : lookup.rows) {
        ASSERT_EQ(partition, table.getRadixPartition(lookup.hashes[row]));
      }
    }

    ASSERT_EQ(totalRows, expectedRows);
  }
};

TEST_F(RadixPartitionerTest, wrapped) {
  auto table = makeRadixTable();
  auto partitioner = RadixPartitioner::createWrapped(*table, 1, pool());

  auto first = makeRowVector(
      std::vector<VectorPtr>{makeFlatVector<int64_t>(128, [](auto row) {
        return row;
      })});
  auto second = makeRowVector(
      std::vector<VectorPtr>{makeFlatVector<int64_t>(128, [](auto row) {
        return 128 + row;
      })});

  partitioner->addInput(first);
  ASSERT_TRUE(partitioner->hasReadyOutput());
  partitioner->addInput(second);
  assertPartitionedOutput(*table, *partitioner, 256, true);
}

TEST_F(RadixPartitionerTest, copied) {
  auto table = makeRadixTable();
  auto partitioner = RadixPartitioner::createCopied(*table, 1'000, pool());

  auto input = makeRowVector(
      std::vector<VectorPtr>{makeFlatVector<int64_t>(64, [](auto row) {
        return row;
      })});

  partitioner->addInput(input);
  assertPartitionedOutput(*table, *partitioner, 64, false);
}

} // namespace
} // namespace facebook::velox::exec::test
