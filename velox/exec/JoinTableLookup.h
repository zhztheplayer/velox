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

#include "velox/exec/HashTable.h"

namespace facebook::velox::exec {

// Lightweight routing API for join callers. The initial implementation always
// returns one table. Future implementations can route by hash to one of many
// backing tables without changing the caller contract.
class JoinTableLookup {
 public:
  virtual ~JoinTableLookup() = default;

  virtual BaseHashTable& table(uint64_t hash) = 0;
  virtual const BaseHashTable& table(uint64_t hash) const = 0;
};

class SingleJoinTableLookup final : public JoinTableLookup {
 public:
  explicit SingleJoinTableLookup(std::shared_ptr<BaseHashTable> table)
      : table_(std::move(table)) {
    VELOX_CHECK_NOT_NULL(table_);
  }

  BaseHashTable& table(uint64_t /*hash*/) override {
    return *table_;
  }

  const BaseHashTable& table(uint64_t /*hash*/) const override {
    return *table_;
  }

  const std::shared_ptr<BaseHashTable>& hashTable() const {
    return table_;
  }

 private:
  std::shared_ptr<BaseHashTable> table_;
};

} // namespace facebook::velox::exec
