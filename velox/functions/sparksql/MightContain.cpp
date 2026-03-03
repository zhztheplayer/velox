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
#include "velox/functions/sparksql/MightContain.h"

namespace facebook::velox::functions::sparksql {

BloomFilterMightContainFunction::BloomFilterMightContainFunction(
    const std::vector<exec::VectorFunctionArg>& inputArgs)
    : inputArgs_(inputArgs) {
  VELOX_CHECK_EQ(inputArgs_.size(), 2);
  auto& bloomFilterArg = inputArgs[0].constantValue;
  VELOX_CHECK(bloomFilterArg != nullptr && !bloomFilterArg->isNullAt(0),
    "BloomFilterMightContain's first argument must be a constant non-null value");

  // Decodes the constant vector.
  SelectivityVector rows(1);
  DecodedVector decodedBloomFilter(*bloomFilterArg, rows);
  auto bloomFilterData = decodedBloomFilter.valueAt<StringView>(0);
  bloomFilterView_ = std::make_unique<BloomFilterView>(bloomFilterData.data());
}

void BloomFilterMightContainFunction::apply(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args,
    const TypePtr& outputType,
    exec::EvalCtx& context,
    VectorPtr& result) const {
  VELOX_CHECK_EQ(args.size(), 2);
  auto& inputArg = args[1];
  BaseVector::ensureWritable(rows, BOOLEAN(), context.pool(), result);

  // Prepares the result data.
  auto resultVector = result->asUnchecked<FlatVector<bool>>();
  auto* resultData = resultVector->mutableRawValues();

  // Decodes input vector.
  exec::DecodedArgs decodedArgs(rows, {inputArg}, context);
  auto inputData = decodedArgs.at(0)->data<int64_t>();

  rows.applyToSelected([&](vector_size_t i) {
    auto inputValue = inputData[i];
    auto outputValue = bloomFilterView_->mayContain(folly::hasher<int64_t>()(inputValue));
    bits::setBit(reinterpret_cast<uint64_t*>(resultData), i, outputValue);
  });
}

bool BloomFilterMightContainFunction::supportsFlatNoNullsFastPath() const {
  return true;
}

std::vector<std::shared_ptr<exec::FunctionSignature>> mightContainSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("boolean")
              .argumentType("varbinary")
              .argumentType("bigint")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeMightContain(
    const std::string&,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig&) {
  return std::make_shared<BloomFilterMightContainFunction>(inputArgs);
}

exec::VectorFunctionMetadata mightContainMetadata() {
  return exec::VectorFunctionMetadataBuilder()
      .build();
}
} // namespace facebook::velox::functions::sparksql

