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

#include <functional>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/benchmarks/ExpressionBenchmarkBuilder.h"
#include "velox/functions/sparksql/registration/Register.h"

using namespace facebook;
using namespace facebook::velox;

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});

  ExpressionBenchmarkBuilder benchmarkBuilder;
  functions::sparksql::registerFunctions("");

  constexpr vector_size_t kVectorSize = 10'000;
  auto vectorMaker = benchmarkBuilder.vectorMaker();
  for (const bool nullable : {false, true}) {
    std::function<bool(vector_size_t)> isNullAt;
    if (nullable) {
      isNullAt = [](auto row) { return row % 10 == 0; };
    }
    auto quantity = vectorMaker.flatVector<int32_t>(
        kVectorSize, [](auto row) { return row % 100 + 1; }, isNullAt);
    auto decimalQuantity = vectorMaker.flatVector<int64_t>(
        kVectorSize,
        [](auto row) { return row % 100 + 1; },
        isNullAt,
        DECIMAL(10, 0));
    auto salesPrice = vectorMaker.flatVector<int64_t>(
        kVectorSize,
        [](auto row) { return 100 + row % 10'000; },
        nullptr,
        DECIMAL(7, 2));

    // Spark TPC-DS plans commonly cast integer quantities to DECIMAL(10, 0)
    // before multiplying by a decimal price. Compare this workload with an
    // equivalent pre-cast input to isolate the runtime cast overhead.
    benchmarkBuilder
        .addBenchmarkSet(
            nullable ? "decimal arithmetic nullable" : "decimal arithmetic",
            vectorMaker.rowVector(
                {"quantity", "decimal_quantity", "sales_price"},
                {quantity, decimalQuantity, salesPrice}))
        .addExpression(
            "multiply_with_cast",
            "multiply(cast(quantity as decimal(10, 0)), sales_price)")
        .addExpression(
            "multiply_precast", "multiply(decimal_quantity, sales_price)")
        .withIterations(1'000);
  }

  benchmarkBuilder.registerBenchmarks();
  folly::runBenchmarks();
  return 0;
}
