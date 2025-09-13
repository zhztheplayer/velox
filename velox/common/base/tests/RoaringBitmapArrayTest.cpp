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

#include "velox/common/base/RoaringBitmapArray.h"
#include <gtest/gtest.h>

namespace facebook::velox {
namespace {
TEST(RoaringBitmapArrayTest, contains) {
  common::RoaringBitmapArray array{};
  array.add(206LL);
  array.add(10LL << 32 | 10LL);
  EXPECT_TRUE(array.contains(206LL));
  EXPECT_FALSE(array.contains(207LL));
  EXPECT_TRUE(array.contains(10LL << 32 | 10LL));
  EXPECT_FALSE(array.contains(11LL << 32 | 10LL));
  EXPECT_FALSE(array.contains(10LL << 32 | 11LL));
}

TEST(RoaringBitmapArrayTest, serde) {
  common::RoaringBitmapArray array{};
  array.add(206LL);
  array.add(10LL << 32 | 10LL);
  std::string data;
  data.resize(array.serializedSizeInBytes());
  array.serialize(data.data());
  common::RoaringBitmapArray deserialized{};
  deserialized.deserialize(data.data());
  EXPECT_TRUE(deserialized.contains(206LL));
  EXPECT_FALSE(deserialized.contains(207LL));
  EXPECT_TRUE(deserialized.contains(10LL << 32 | 10LL));
  EXPECT_FALSE(deserialized.contains(11LL << 32 | 10LL));
  EXPECT_FALSE(deserialized.contains(10LL << 32 | 11LL));
}
} // namespace
} // namespace facebook::velox
