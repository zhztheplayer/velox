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

#include <functions/delta/RoaringBitmapArray.h>

#include "velox/core/Expressions.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class RoaringBitmapArrayTest : public ::testing::Test {};

TEST_F(RoaringBitmapArrayTest, contains) {
  RoaringBitmapArray array{};
  array.add(206L);
  array.add(10L << 32 | 10L);
  EXPECT_TRUE(array.contains(206L));
  EXPECT_FALSE(array.contains(207L));
  EXPECT_TRUE(array.contains(10L << 32 | 10));
  EXPECT_FALSE(array.contains(11L << 32 | 10));
  EXPECT_FALSE(array.contains(10L << 32 | 11));
}

TEST_F(RoaringBitmapArrayTest, serde) {
  RoaringBitmapArray array{};
  array.add(206L);
  array.add(10L << 32 | 10L);
  std::string data;
  data.resize(array.serializedSizeInBytes());
  array.serialize(data.data());
  RoaringBitmapArray deserialized{};
  deserialized.deserialize(data.data());
  EXPECT_TRUE(deserialized.contains(206L));
  EXPECT_FALSE(deserialized.contains(207L));
  EXPECT_TRUE(deserialized.contains(10L << 32 | 10));
  EXPECT_FALSE(deserialized.contains(11L << 32 | 10));
  EXPECT_FALSE(deserialized.contains(10L << 32 | 11));
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
