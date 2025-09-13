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

#include <memory>
#include <roaring/roaring.hh>

namespace facebook::velox::common {

/// The C++ implementation of 64-bit array-based roaring bitmap which
/// is used by Delta Lake deletion vectors. See
/// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vector-format.
class RoaringBitmapArray {
 public:
  RoaringBitmapArray() = default;

  void deserialize(const char* serialized);

  void serialize(char* buf) const;

  void add(int64_t value);

  bool contains(int64_t value) const;

  int64_t serializedSizeInBytes() const;

 private:
  static void checkValue(int64_t value);

  // Extracts the high 32 bits. The function doesn't check
  // the input value internally. Call `checkValue` first
  // before calling the function.
  static int32_t highBytesUnsafe(int64_t value);

  // Extracts the low 32 bits. The function doesn't check
  // the input value internally. Call `checkValue` first
  // before calling the function.
  static int32_t lowBytesUnsafe(int64_t value);

  static constexpr int32_t kPortableSerializationFormatMagicNumber{1681511377};
  std::vector<std::shared_ptr<roaring::Roaring>> bitmaps_{};
  std::vector<std::shared_ptr<roaring::BulkContext>> buckContexts_{};
  mutable int32_t lastHighBytes_{-1};
  mutable roaring::Roaring* lastBitmap_{nullptr};
  mutable roaring::BulkContext* lastContext_{nullptr};
};

} // namespace facebook::velox::common
