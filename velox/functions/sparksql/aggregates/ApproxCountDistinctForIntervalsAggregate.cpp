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

#include "velox/functions/sparksql/aggregates/ApproxCountDistinctForIntervalsAggregate.h"
#include "velox/expression/FunctionSignature.h"

namespace facebook::velox::functions::aggregate::sparksql {

namespace {

std::vector<double> extractEndpoints(const VectorPtr& endpointsVector) {
  VELOX_USER_CHECK_NOT_NULL(endpointsVector, "Endpoints cannot be null");
  
  auto* arrayVector = endpointsVector->as<ArrayVector>();
  VELOX_USER_CHECK_NOT_NULL(arrayVector, "Endpoints must be an array");
  VELOX_USER_CHECK_EQ(arrayVector->size(), 1, "Expected single endpoints array");
  
  auto offset = arrayVector->offsetAt(0);
  auto size = arrayVector->sizeAt(0);
  
  VELOX_USER_CHECK_GE(size, 2, "Endpoints array must have at least 2 elements");
  
  std::vector<double> endpoints;
  endpoints.reserve(size);
  
  auto* elements = arrayVector->elements().get();
  
  for (vector_size_t i = 0; i < size; ++i) {
    VELOX_USER_CHECK(
        !elements->isNullAt(offset + i),
        "Endpoints array cannot contain null values");
    
    // Convert element to double based on type
    if (elements->typeKind() == TypeKind::DOUBLE) {
      endpoints.push_back(elements->asFlatVector<double>()->valueAt(offset + i));
    } else if (elements->typeKind() == TypeKind::REAL) {
      endpoints.push_back(
          static_cast<double>(elements->asFlatVector<float>()->valueAt(offset + i)));
    } else if (elements->typeKind() == TypeKind::BIGINT) {
      endpoints.push_back(
          static_cast<double>(elements->asFlatVector<int64_t>()->valueAt(offset + i)));
    } else if (elements->typeKind() == TypeKind::INTEGER) {
      endpoints.push_back(
          static_cast<double>(elements->asFlatVector<int32_t>()->valueAt(offset + i)));
    } else if (elements->typeKind() == TypeKind::TIMESTAMP) {
      endpoints.push_back(
          static_cast<double>(elements->asFlatVector<Timestamp>()->valueAt(offset + i).toMicros()));
    } else {
      VELOX_USER_FAIL("Unsupported endpoint type: {}", elements->type()->toString());
    }
  }
  
  return endpoints;
}

} // namespace

void registerApproxCountDistinctForIntervalsAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("array(bigint)")
          .intermediateType("array(array(varbinary))")
          .argumentType("T")
          .argumentType("array(double)")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("array(bigint)")
          .intermediateType("array(array(varbinary))")
          .argumentType("T")
          .argumentType("array(double)")
          .argumentType("double")
          .build()};

  exec::registerAggregateFunction(
      {prefix + "approx_count_distinct_for_intervals"},
      std::move(signatures),
      [](
          core::AggregationNode::Step /*step*/,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_USER_CHECK(
            argTypes.size() == 2 || argTypes.size() == 3,
            "approx_count_distinct_for_intervals requires 2 or 3 arguments");
        
        // Default relative standard deviation (matches Spark's default)
        double relativeSD = 0.05;
        if (argTypes.size() == 3) {
          // TODO: Extract relativeSD from constant argument
          // For now, use default value
        }
        
        // TODO: Extract endpoints from constant array argument
        // This is a placeholder - actual implementation would need to extract
        // the constant value from the query plan during function instantiation
        std::vector<double> endpoints = {0.0, 100.0}; // Placeholder
        
        auto inputType = argTypes[0];
        return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            createApproxCountDistinctForIntervalsAggregate,
            inputType->kind(),
            resultType,
            endpoints,
            relativeSD);
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::functions::aggregate::sparksql
