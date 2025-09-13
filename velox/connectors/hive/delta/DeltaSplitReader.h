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

#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/SplitReader.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/delta/DeltaSplit.h"

namespace facebook::velox::connector::hive::delta {

class DeltaSplitReader : public SplitReader {
 public:
  DeltaSplitReader(
      const std::shared_ptr<const hive::HiveConnectorSplit>& hiveSplit,
      const HiveTableHandlePtr& hiveTableHandle,
      const std::unordered_map<std::string, HiveColumnHandlePtr>* partitionKeys,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<const HiveConfig>& hiveConfig,
      const RowTypePtr& readerOutputType,
      const std::shared_ptr<io::IoStatistics>& ioStats,
      const std::shared_ptr<filesystems::File::IoStats>& fsStats,
      FileHandleFactory* fileHandleFactory,
      folly::Executor* executor,
      const std::shared_ptr<common::ScanSpec>& scanSpec)
      : SplitReader(
            hiveSplit,
            hiveTableHandle,
            partitionKeys,
            connectorQueryCtx,
            hiveConfig,
            readerOutputType,
            ioStats,
            fsStats,
            fileHandleFactory,
            executor,
            scanSpec) {
    std::shared_ptr<const HiveDeltaSplit> deltaSplit =
        std::dynamic_pointer_cast<const HiveDeltaSplit>(hiveSplit_);
    VELOX_CHECK(
        deltaSplit->tableBucketNumber == std::nullopt &&
            deltaSplit->bucketConversion == std::nullopt,
        "Delta Lake reader doesn't support bucketing");
    VELOX_CHECK_EQ(
        deltaSplit->start,
        0,
        "Delta Lake reader doesn't support reading from an offset");
  };

  uint64_t next(uint64_t size, VectorPtr& output) override {
    uint64_t numScanned;
    std::shared_ptr<const HiveDeltaSplit> deltaSplit =
        std::dynamic_pointer_cast<const HiveDeltaSplit>(hiveSplit_);

    dwio::common::Mutation mutation;
    if (baseReaderOpts_.randomSkip()) {
      mutation.randomSkip = baseReaderOpts_.randomSkip().get();
    }
    if (deltaSplit->rowIndexFilter.has_value()) {
      dwio::common::ensureCapacity<int8_t>(
          deleteBitmap_, size, connectorQueryCtx_->memoryPool(), false, true);
      deltaSplit->rowIndexFilter->materializeIntoBuffer(
          numRowsRead_, numRowsRead_ + size, deleteBitmap_);
      mutation.deletedRows = deleteBitmap_->as<uint64_t>();
    }
    numScanned = baseRowReader_->next(size, output, &mutation);
    numRowsRead_ += numScanned;

    return numScanned;
  }

 private:
  uint64_t numRowsRead_ = 0;
  BufferPtr deleteBitmap_;
};
} // namespace facebook::velox::connector::hive::delta
