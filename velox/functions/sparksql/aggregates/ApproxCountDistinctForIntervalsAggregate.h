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
#pragma once

#include <algorithm>
#include "velox/common/hyperloglog/HllUtils.h"
#include "velox/common/hyperloglog/SparseHll.h"
#include "velox/exec/Aggregate.h"
#include "velox/functions/lib/HllAccumulator.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::aggregate::sparksql {

template <typename T>
class ApproxCountDistinctForIntervalsAggregate : public exec::Aggregate {
 public:
  explicit ApproxCountDistinctForIntervalsAggregate(
      const TypePtr& resultType,
      const std::vector<double>& endpoints,
      double relativeSD)
      : exec::Aggregate(resultType),
        endpoints_(endpoints),
        indexBitLength_(common::hll::toIndexBitLength(relativeSD)),
        numIntervals_(endpoints.size() - 1) {
    VELOX_USER_CHECK_GE(
        endpoints.size(),
        2,
        "Endpoints array must have at least 2 elements");
    
    // Verify endpoints are sorted
    for (size_t i = 1; i < endpoints.size(); ++i) {
      VELOX_USER_CHECK_GE(
          endpoints[i],
          endpoints[i - 1],
          "Endpoints must be sorted in ascending order");
    }
  }

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(IntervalAccumulator);
  }

  int32_t accumulatorAlignmentSize() const override {
    return alignof(IntervalAccumulator);
  }

  bool isFixedSize() const override {
    return false;
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(result);
    auto* arrayVector = (*result)->as<ArrayVector>();
    VELOX_CHECK_NOT_NULL(arrayVector);
    
    arrayVector->resize(numGroups);
    auto* offsets = arrayVector->mutableOffsets(numGroups)->asMutable<vector_size_t>();
    auto* sizes = arrayVector->mutableSizes(numGroups)->asMutable<vector_size_t>();
    auto* elements = arrayVector->elements()->asFlatVector<int64_t>();
    
    elements->resize(numGroups * numIntervals_);
    
    uint64_t* rawNulls = getRawNulls(arrayVector);
    
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        arrayVector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        auto* accumulator = value<IntervalAccumulator>(group);
        
        offsets[i] = i * numIntervals_;
        sizes[i] = numIntervals_;
        
        // Extract cardinality from each HLL accumulator
        for (size_t j = 0; j < numIntervals_; ++j) {
          int64_t cardinality = accumulator->hllAccumulators[j].cardinality();
          
          // Handle duplicate endpoints: set ndv=1 for intervals between them
          if (endpoints_[j] == endpoints_[j + 1]) {
            cardinality = 1;
          }
          
          elements->set(i * numIntervals_ + j, cardinality);
        }
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedValue_.decode(*args[0], rows);
    
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedValue_.isNullAt(row)) {
        return;
      }
      
      auto group = groups[row];
      auto tracker = trackRowSize(group);
      clearNull(group);
      
      auto* accumulator = value<IntervalAccumulator>(group);
      
      // Convert value to double for interval search
      double doubleValue = convertToDouble(decodedValue_.valueAt<T>(row));
      
      // Check if value is within the overall range
      if (doubleValue < endpoints_.front() || doubleValue > endpoints_.back()) {
        return;
      }
      
      // Find which interval this value belongs to
      size_t intervalIndex = findIntervalIndex(doubleValue);
      
      // Add value to the corresponding HLL accumulator
      accumulator->hllAccumulators[intervalIndex].setIndexBitLength(indexBitLength_);
      accumulator->hllAccumulators[intervalIndex].append(decodedValue_.valueAt<T>(row));
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedIntermediate_.decode(*args[0], rows);
    auto* arrayVector = decodedIntermediate_.base()->as<ArrayVector>();
    
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedIntermediate_.isNullAt(row)) {
        return;
      }
      
      auto group = groups[row];
      auto tracker = trackRowSize(group);
      clearNull(group);
      
      auto decodedRow = decodedIntermediate_.index(row);
      auto offset = arrayVector->offsetAt(decodedRow);
      auto size = arrayVector->sizeAt(decodedRow);
      
      VELOX_CHECK_EQ(
          size,
          numIntervals_,
          "Intermediate array size must match number of intervals");
      
      auto* accumulator = value<IntervalAccumulator>(group);
      auto* elements = arrayVector->elements()->as<ArrayVector>();
      
      // Merge each HLL accumulator
      for (size_t i = 0; i < numIntervals_; ++i) {
        auto hllOffset = elements->offsetAt(offset + i);
        auto hllSize = elements->sizeAt(offset + i);
        
        if (hllSize > 0) {
          auto* hllData = elements->elements()->asFlatVector<StringView>();
          auto serialized = hllData->valueAt(hllOffset);
          accumulator->hllAccumulators[i].mergeWith(serialized, allocator_);
        }
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedValue_.decode(*args[0], rows);
    auto tracker = trackRowSize(group);
    
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedValue_.isNullAt(row)) {
        return;
      }
      
      clearNull(group);
      auto* accumulator = value<IntervalAccumulator>(group);
      
      double doubleValue = convertToDouble(decodedValue_.valueAt<T>(row));
      
      if (doubleValue < endpoints_.front() || doubleValue > endpoints_.back()) {
        return;
      }
      
      size_t intervalIndex = findIntervalIndex(doubleValue);
      
      accumulator->hllAccumulators[intervalIndex].setIndexBitLength(indexBitLength_);
      accumulator->hllAccumulators[intervalIndex].append(decodedValue_.valueAt<T>(row));
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addIntermediateResults(&group, rows, args, false);
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      auto group = groups[i];
      new (group + offset_) IntervalAccumulator(numIntervals_, allocator_);
    }
  }

  void destroyInternal(folly::Range<char**> groups) override {
    for (auto group : groups) {
      if (isInitialized(group)) {
        auto* accumulator = value<IntervalAccumulator>(group);
        std::destroy_at(accumulator);
      }
    }
  }

 private:
  struct IntervalAccumulator {
    std::vector<velox::common::hll::HllAccumulator<T, false>> hllAccumulators;
    
    IntervalAccumulator(size_t numIntervals, HashStringAllocator* allocator) {
      hllAccumulators.reserve(numIntervals);
      for (size_t i = 0; i < numIntervals; ++i) {
        hllAccumulators.emplace_back(allocator);
      }
    }
  };

  // Convert value to double for interval comparison
  double convertToDouble(const T& value) const {
    if constexpr (std::is_integral_v<T>) {
      return static_cast<double>(value);
    } else if constexpr (std::is_floating_point_v<T>) {
      return static_cast<double>(value);
    } else if constexpr (std::is_same_v<T, Timestamp>) {
      return static_cast<double>(value.toMicros());
    } else {
      VELOX_UNSUPPORTED("Unsupported type for interval comparison");
    }
  }

  // Find which interval the value belongs to using binary search
  size_t findIntervalIndex(double value) const {
    // Binary search to find the interval
    auto it = std::upper_bound(endpoints_.begin(), endpoints_.end(), value);
    
    if (it == endpoints_.begin()) {
      return 0;
    }
    
    size_t index = std::distance(endpoints_.begin(), it) - 1;
    
    // Handle exact matches at endpoints
    if (index > 0 && endpoints_[index] == value) {
      // Move to first occurrence of this value
      while (index > 0 && endpoints_[index - 1] == value) {
        --index;
      }
      if (index > 0) {
        --index;
      }
    }
    
    return std::min(index, numIntervals_ - 1);
  }

  const std::vector<double> endpoints_;
  const int8_t indexBitLength_;
  const size_t numIntervals_;
  
  DecodedVector decodedValue_;
  DecodedVector decodedIntermediate_;
};

template <TypeKind kind>
std::unique_ptr<exec::Aggregate> createApproxCountDistinctForIntervalsAggregate(
    const TypePtr& resultType,
    const std::vector<double>& endpoints,
    double relativeSD) {
  using T = typename TypeTraits<kind>::NativeType;
  return std::make_unique<ApproxCountDistinctForIntervalsAggregate<T>>(
      resultType, endpoints, relativeSD);
}

void registerApproxCountDistinctForIntervalsAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);

} // namespace facebook::velox::functions::aggregate::sparksql
