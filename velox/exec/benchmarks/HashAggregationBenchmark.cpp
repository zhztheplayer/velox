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
#include <iostream>

#include "velox/common/memory/Memory.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::test;

namespace {

constexpr vector_size_t kBatchSize = 10'000;
constexpr int32_t kNumBatches = 1'000;

class HashAggregationBenchmark : public VectorTestBase {
 public:
  HashAggregationBenchmark() : data_(makeData()) {
    plan_ = PlanBuilder()
                .values(data_)
                .partialAggregation({"k"}, {"sum(v)"})
                .capturePlanNodeId(partialAggregationId_)
                .finalAggregation()
                .singleAggregation({}, {"count(1)"})
                .planNode();
  }

  void addBenchmark(int32_t abandonMinRows) {
    folly::addBenchmark(
        __FILE__,
        "partial_aggregation_abandon_after_" +
            std::to_string(abandonMinRows) + "_rows",
        [this, abandonMinRows]() {
          std::shared_ptr<Task> task;
          AssertQueryBuilder(plan_)
              .serialExecution(true)
              .config(
                  core::QueryConfig::kAbandonPartialAggregationMinRows,
                  abandonMinRows)
              .config(core::QueryConfig::kAbandonPartialAggregationMinPct, 80)
              .config(core::QueryConfig::kMaxPartialAggregationMemory, 1UL << 30)
              .config(
                  core::QueryConfig::kMaxExtendedPartialAggregationMemory,
                  1UL << 30)
              .countResults(task);

          BENCHMARK_SUSPEND {
            const auto pairs = toPlanStats(task->taskStats());
            const auto& stats =
                pairs.at(partialAggregationId_);
            std::cout << "abandon after " << abandonMinRows
                      << " rows, partial aggregation addInput wall time: "
                      << stats.addInputTiming.wallNanos << "ns\n";
          }
          return 1;
        });
  }

 private:
  std::vector<RowVectorPtr> makeData() {
    std::vector<RowVectorPtr> data;
    data.reserve(kNumBatches);
    for (int32_t batch = 0; batch < kNumBatches; ++batch) {
      data.push_back(makeRowVector(
          {"k", "v"},
          {makeFlatVector<int64_t>(kBatchSize, [batch](auto row) {
             return batch * kBatchSize + row;
           }),
           makeFlatVector<int64_t>(kBatchSize, [](auto row) { return row; })}));
    }
    return data;
  }

  std::vector<RowVectorPtr> data_;
  core::PlanNodePtr plan_;
  core::PlanNodeId partialAggregationId_;
};

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  aggregate::prestosql::registerAllAggregateFunctions();

  HashAggregationBenchmark benchmark;
  for (const auto minRows : {100, 10'000, 100'000}) {
    benchmark.addBenchmark(minRows);
  }
  folly::runBenchmarks();
  return 0;
}
