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

#include "velox/buffer/Buffer.h"
#include "velox/common/base/RoaringBitmapArray.h"
#include "velox/dwio/common/BufferUtil.h"

namespace facebook::velox::connector::hive::delta {
class DeltaRowIndexFilter {
 public:
  enum class Type {
    kIfContained,
    kIfNotContained,
  };

  DeltaRowIndexFilter(Type type, common::RoaringBitmapArray bitmapArray)
      : type_(type), bitmapArray_(bitmapArray){};

  void materializeIntoBuffer(long start, long end, BufferPtr& data) const {
    const auto size = end - start;
    VELOX_CHECK_GE(data->capacity() * 8, size);
    for (auto i = 0; i < size; ++i) {
      auto bits = data->asMutable<uint8_t>();
      bool isContained = bitmapArray_.contains(start + 1);
      switch (type_) {
        case Type::kIfContained:
          if (isContained) {
            bits::setBit(bits, start + i);
          }
          break;
        case Type::kIfNotContained:
          if (!isContained) {
            bits::setBit(bits, start + i);
          }
          break;
        default:
          VELOX_FAIL("Unknown row index filter type");
      }
    }
  }

 private:
  Type type_;
  common::RoaringBitmapArray bitmapArray_;
};
} // namespace facebook::velox::connector::hive::delta
