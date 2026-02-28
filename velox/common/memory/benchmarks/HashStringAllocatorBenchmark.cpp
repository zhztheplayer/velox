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

#include <algorithm>
#include <array>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/common/memory/Memory.h"

using namespace facebook::velox;

namespace {

constexpr int32_t kNumStrings = 256;
constexpr std::array<int32_t, 11> kAvgTextSizes = {
    12, 24, 48, 96, 192, 384, 1024, 2 * 1024, 4 * 1024, 16 * 1024, 64 * 1024};
constexpr std::array<int32_t, 7> kPartSizes = {
    24, 48, 96, 192, 384, 1024, 2 * 1024};

uint64_t packConfig(int32_t avgTextSize, int32_t partSize) {
  return (static_cast<uint64_t>(avgTextSize) << 32) |
      static_cast<uint32_t>(partSize);
}

std::pair<int32_t, int32_t> unpackConfig(uint64_t packed) {
  return {static_cast<int32_t>(packed >> 32), static_cast<int32_t>(packed)};
}

std::vector<std::string> makeTexts(int32_t avgTextSize, int32_t partSize) {
  std::mt19937 rng(42 + avgTextSize + partSize);
  const int32_t minSize = std::max(8, partSize + 1);
  const int32_t maxSize = std::max(minSize, avgTextSize + avgTextSize / 2);
  std::uniform_int_distribution<int32_t> sizeDist(minSize, maxSize);
  std::uniform_int_distribution<int32_t> charDist(32, 126);

  std::vector<std::string> texts;
  texts.reserve(kNumStrings);
  for (int32_t i = 0; i < kNumStrings; ++i) {
    std::string text(sizeDist(rng), 0);
    for (auto& c : text) {
      c = static_cast<char>(charDist(rng));
    }
    texts.push_back(std::move(text));
  }
  return texts;
}

HashStringAllocator::Position storeMultipart(
    HashStringAllocator& allocator,
    ByteOutputStream& stream,
    const std::string& text,
    int32_t partSize) {
  auto start = allocator.newWrite(stream, std::max(1, partSize));
  stream.appendStringView(std::string_view(text));
  return allocator.finishWrite(stream, 0).first;
}

void benchmarkEndToEndMultipart(uint32_t n, uint64_t packedConfig) {
  const auto [avgTextSize, partSize] = unpackConfig(packedConfig);
  std::shared_ptr<memory::MemoryPool> pool;
  std::vector<std::string> texts;

  {
    folly::BenchmarkSuspender suspender;
    pool = memory::memoryManager()->addLeafPool();
    texts = makeTexts(avgTextSize, partSize);
    suspender.dismiss();
  }

  HashStringAllocator allocator(pool.get());
  ByteOutputStream stream(&allocator);
  std::string storage;
  for (uint32_t i = 0; i < n; ++i) {
    for (const auto& text : texts) {
      auto start = storeMultipart(allocator, stream, text, partSize);
      auto multipart =
          StringView(reinterpret_cast<const char*>(start.header->begin()), text.size());
      auto contiguous = HashStringAllocator::contiguousString(multipart, storage);
      folly::doNotOptimizeAway(contiguous.data());
      allocator.free(start.header);
    }
  }
}

std::string sizeLabel(int32_t size) {
  if (size % 1024 == 0) {
    return std::to_string(size / 1024) + "k";
  }
  return std::to_string(size);
}

void registerMultipartBenchmarks() {
  for (size_t avgIdx = 0; avgIdx < kAvgTextSizes.size(); ++avgIdx) {
    const auto avgTextSize = kAvgTextSizes[avgIdx];
    for (const auto partSize : kPartSizes) {
      const auto name = "benchmarkEndToEndMultipart(avg" + sizeLabel(avgTextSize) +
          "_part" + sizeLabel(partSize) + ")";
      folly::addBenchmark(
          __FILE__,
          name,
          [config = packConfig(avgTextSize, partSize)](unsigned int n) {
            benchmarkEndToEndMultipart(n, config);
            return n;
          });
    }
    if (avgIdx + 1 < kAvgTextSizes.size()) {
      folly::addBenchmark(__FILE__, "-", []() -> unsigned { return 0; });
    }
  }
}
} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  registerMultipartBenchmarks();
  folly::runBenchmarks();
  return 0;
}
