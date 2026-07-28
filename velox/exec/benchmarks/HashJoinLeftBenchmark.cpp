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

#include "velox/common/memory/Memory.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::test;

namespace {

constexpr vector_size_t kBatchSize = 100'000;
constexpr int64_t kNumProbeRows = 50'000'000;
constexpr uint64_t kBloomFilterMaxBytes = 256UL << 20;

struct BenchmarkParams {
  int64_t numBuildRows;
  int32_t hitPct;
  bool enableBloomFilter;
};

struct BenchmarkData {
  std::vector<RowVectorPtr> buildVectors;
  std::vector<RowVectorPtr> probeVectors;
};

// Maps nearby row numbers to keys spread across the BIGINT range. Build keys
// are even and miss keys are odd, making the requested hit rate exact while
// avoiding artificial locality in the hash table.
uint64_t mix(uint64_t value) {
  value += 0x9e3779b97f4a7c15;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9;
  value = (value ^ (value >> 27)) * 0x94d049bb133111eb;
  return value ^ (value >> 31);
}

int64_t buildKey(uint64_t row) {
  return static_cast<int64_t>(mix(row) & ~uint64_t{1});
}

class HashJoinLeftBenchmark : public VectorTestBase {
 public:
  BenchmarkData prepareData(const BenchmarkParams& params) {
    BenchmarkData data;
    data.buildVectors =
        makeBatches(params.numBuildRows, [&](int64_t row) {
          return makeRowVector(
              {"u0", "u1"},
              {
                  makeFlatVector<int64_t>(
                      rowCount(row, params.numBuildRows),
                      [&](vector_size_t index) {
                        return buildKey(row + index);
                      }),
                  makeFlatVector<int64_t>(
                      rowCount(row, params.numBuildRows),
                      [&](vector_size_t index) { return row + index; }),
              });
        });

    data.probeVectors = makeBatches(kNumProbeRows, [&](int64_t row) {
      const auto size = rowCount(row, kNumProbeRows);
      return makeRowVector(
          {"t0"},
          {makeFlatVector<int64_t>(size, [&](vector_size_t index) {
            const auto probeRow = row + index;
            const auto random = mix(probeRow);
            if (random % 100 < params.hitPct) {
              return buildKey(random % params.numBuildRows);
            }
            return static_cast<int64_t>(random | uint64_t{1});
          })});
    });
    return data;
  }

  uint64_t run(const BenchmarkParams& params, const BenchmarkData& data) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan =
        PlanBuilder(planNodeIdGenerator, pool_.get())
            .values(data.probeVectors)
            .hashJoin(
                {"t0"},
                {"u0"},
                PlanBuilder(planNodeIdGenerator, pool_.get())
                    .values(data.buildVectors)
                    .planNode(),
                "",
                {"t0", "u1"},
                core::JoinType::kLeft)
            .planNode();

    AssertQueryBuilder query(plan);
    query.maxDrivers(1)
        .config(
            core::QueryConfig::kHashProbeBloomFilterPushdownMaxSize,
            std::to_string(kBloomFilterMaxBytes))
        .config(
            core::QueryConfig::kBypassHashProbeBloomFilterMinRows,
            params.enableBloomFilter ? std::to_string(100'000) : std::to_string(0))
        .config(core::QueryConfig::kBypassHashProbeBloomFilterMinPct, std::to_string(85));
    return query.countResults();
  }

 private:
  static vector_size_t rowCount(int64_t offset, int64_t totalRows) {
    return static_cast<vector_size_t>(
        std::min<int64_t>(kBatchSize, totalRows - offset));
  }

  template <typename MakeBatch>
  std::vector<RowVectorPtr> makeBatches(
      int64_t numRows,
      MakeBatch makeBatch) {
    std::vector<RowVectorPtr> batches;
    batches.reserve((numRows + kBatchSize - 1) / kBatchSize);
    for (int64_t row = 0; row < numRows; row += kBatchSize) {
      batches.push_back(makeBatch(row));
    }
    return batches;
  }
};

std::string benchmarkName(const BenchmarkParams& params) {
  return fmt::format(
      "build_{}M_probe_50M_hit_{}pct_bloom_{}",
      params.numBuildRows / 1'000'000,
      params.hitPct,
      params.enableBloomFilter ? "enabled" : "disabled");
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  functions::prestosql::registerAllScalarFunctions();
  parse::registerTypeResolver();

  auto benchmark = std::make_unique<HashJoinLeftBenchmark>();
  for (const auto numBuildRows : {1'000'000, 10'000'000}) {
    for (const auto hitPct : {1, 10, 100}) {
      for (const auto enableBloomFilter : {false, true}) {
        const BenchmarkParams params{
            numBuildRows, hitPct, enableBloomFilter};
        folly::addBenchmark(
            __FILE__,
            benchmarkName(params),
            [benchmark = benchmark.get(), params]() {
              folly::BenchmarkSuspender suspender;
              auto data = benchmark->prepareData(params);
              suspender.dismiss();

              const auto outputRows = benchmark->run(params, data);
              VELOX_CHECK_EQ(outputRows, kNumProbeRows);
              folly::doNotOptimizeAway(outputRows);
              return 1;
            });
      }
    }
  }

  folly::runBenchmarks();
  return 0;
}
