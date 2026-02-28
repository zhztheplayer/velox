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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <cstring>

#include "velox/common/memory/Memory.h"
#include "velox/exec/RowContainer.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

constexpr int32_t kNumRows = 8'192;
constexpr int32_t kInlineSize = 8;
constexpr int32_t kContiguousSize = 64;
constexpr int32_t kMultipartSize = 8 * 1024;

struct DataSet {
  std::unique_ptr<RowContainer> rowContainer;
  std::vector<char*> rows;
  VectorPtr result;
};

class ExtractStringBenchmark {
 public:
  ExtractStringBenchmark() : pool_{memory::memoryManager()->addLeafPool()} {
    setupStored(kInlineSize, inlineData_);
    setupStored(kContiguousSize, contiguousData_);
    setupMultipart(multipartData_);
  }

  void runInline(uint32_t n) {
    run(n, inlineData_);
  }

  void runContiguous(uint32_t n) {
    run(n, contiguousData_);
  }

  void runMultipart(uint32_t n) {
    run(n, multipartData_);
  }

 private:
  void run(uint32_t n, DataSet& dataSet) {
    for (uint32_t i = 0; i < n; ++i) {
      dataSet.result->prepareForReuse();
      dataSet.rowContainer->extractColumn(
          dataSet.rows.data(), dataSet.rows.size(), 0, dataSet.result);
      folly::doNotOptimizeAway(dataSet.result);
    }
  }

  void setupStored(int32_t stringSize, DataSet& dataSet) {
    dataSet.rowContainer =
        std::make_unique<RowContainer>(std::vector<TypePtr>{VARCHAR()}, pool());

    auto input = BaseVector::create<FlatVector<StringView>>(
        VARCHAR(), kNumRows, pool());
    auto* flatInput = input->asFlatVector<StringView>();
    const std::string value(stringSize, 'x');
    for (int32_t i = 0; i < kNumRows; ++i) {
      flatInput->set(i, StringView(value));
    }

    DecodedVector decoded(*input);
    dataSet.rows.resize(kNumRows);
    for (int32_t i = 0; i < kNumRows; ++i) {
      dataSet.rows[i] = dataSet.rowContainer->newRow();
    }
    dataSet.rowContainer->store(
        decoded, folly::Range<char**>(dataSet.rows.data(), dataSet.rows.size()), 0);

    dataSet.result = BaseVector::create<FlatVector<StringView>>(
        VARCHAR(), kNumRows, pool());

    validateStorageMode(stringSize, false, dataSet);
  }

  void setupMultipart(DataSet& dataSet) {
    dataSet.rowContainer =
        std::make_unique<RowContainer>(std::vector<TypePtr>{VARCHAR()}, pool());
    dataSet.rows.resize(kNumRows);
    const auto column = dataSet.rowContainer->columnAt(0);
    for (int32_t i = 0; i < kNumRows; ++i) {
      auto* row = dataSet.rowContainer->newRow();
      dataSet.rowContainer->initializeFields(row);
      *reinterpret_cast<StringView*>(row + column.offset()) =
          makeMultipartString(dataSet.rowContainer->stringAllocator(), kMultipartSize);
      dataSet.rows[i] = row;
    }

    dataSet.result = BaseVector::create<FlatVector<StringView>>(
        VARCHAR(), kNumRows, pool());

    validateStorageMode(kMultipartSize, true, dataSet);
  }

  static StringView firstStoredValue(const DataSet& dataSet) {
    const auto column = dataSet.rowContainer->columnAt(0);
    return *reinterpret_cast<const StringView*>(dataSet.rows.front() + column.offset());
  }

  static void validateStorageMode(
      int32_t stringSize,
      bool expectMultipart,
      const DataSet& dataSet) {
    const auto value = firstStoredValue(dataSet);
    if (stringSize <= StringView::kInlineSize) {
      VELOX_CHECK(value.isInline(), "Expected inline string storage.");
      return;
    }

    VELOX_CHECK(!value.isInline(), "Expected out-of-line string storage.");
    const auto firstPartSize = HashStringAllocator::headerOf(value.data())->size();
    if (expectMultipart) {
      VELOX_CHECK_LT(firstPartSize, value.size(), "Expected multipart storage.");
    } else {
      VELOX_CHECK_GE(
          firstPartSize,
          value.size(),
          "Expected contiguous out-of-line storage.");
    }
  }

  static StringView makeMultipartString(
      HashStringAllocator& allocator,
      int32_t stringSize) {
    VELOX_CHECK_GT(stringSize, 1, "Expected string size > 1.");
    std::string text(stringSize, 'm');

    constexpr int32_t kFirstPartTarget = 1'024;
    auto* first = allocator.allocate(
        kFirstPartTarget + HashStringAllocator::Header::kContinuedPtrSize);
    first->setContinued();
    const int32_t firstPartSize =
        std::min<int32_t>(first->usableSize(), stringSize - 1);
    const int32_t secondPartSize = stringSize - firstPartSize;
    auto* second = allocator.allocate(secondPartSize);

    std::memcpy(first->begin(), text.data(), firstPartSize);
    std::memcpy(second->begin(), text.data() + firstPartSize, secondPartSize);

    *reinterpret_cast<HashStringAllocator::Header**>(
        first->end() - HashStringAllocator::Header::kContinuedPtrSize) = second;
    return StringView(first->begin(), stringSize);
  }

  memory::MemoryPool* pool() const {
    return pool_.get();
  }

  const std::shared_ptr<memory::MemoryPool> pool_;
  DataSet inlineData_;
  DataSet contiguousData_;
  DataSet multipartData_;
};

std::unique_ptr<ExtractStringBenchmark> benchmark;

BENCHMARK(extractStringInline, n) {
  benchmark->runInline(n);
}

BENCHMARK(extractStringContiguous, n) {
  benchmark->runContiguous(n);
}

BENCHMARK(extractStringMultipart, n) {
  benchmark->runMultipart(n);
}

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});

  benchmark = std::make_unique<ExtractStringBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();

  return 0;
}
