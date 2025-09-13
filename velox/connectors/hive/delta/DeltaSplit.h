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

#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/delta/DeltaRowIndexFilter.h"

#include <optional>

namespace facebook::velox::connector::hive::delta {
struct HiveDeltaSplit : HiveConnectorSplit {
  std::optional<DeltaRowIndexFilter> rowIndexFilter;

  HiveDeltaSplit(
      const std::string& connectorId,
      const std::string& filePath,
      dwio::common::FileFormat fileFormat,
      uint64_t start = 0,
      uint64_t length = std::numeric_limits<uint64_t>::max(),
      const std::unordered_map<std::string, std::optional<std::string>>&
          partitionKeys = {},
      std::optional<int32_t> tableBucketNumber = std::nullopt,
      const std::unordered_map<std::string, std::string>& customSplitInfo = {},
      const std::shared_ptr<std::string>& extraFileInfo = {},
      const std::unordered_map<std::string, std::string>& serdeParameters = {},
      int64_t splitWeight = 0,
      bool cacheable = true,
      const std::unordered_map<std::string, std::string>& infoColumns = {},
      std::optional<FileProperties> properties = std::nullopt,
      std::optional<RowIdProperties> rowIdProperties = std::nullopt,
      const std::optional<HiveBucketConversion>& bucketConversion =
          std::nullopt,
      std::optional<DeltaRowIndexFilter> _rowIndexFilter = std::nullopt)
      : HiveConnectorSplit(
            connectorId,
            filePath,
            fileFormat,
            start,
            length,
            partitionKeys,
            tableBucketNumber,
            customSplitInfo,
            extraFileInfo,
            serdeParameters,
            splitWeight,
            cacheable,
            infoColumns,
            properties,
            rowIdProperties,
            bucketConversion),
        rowIndexFilter(_rowIndexFilter) {}
};
} // namespace facebook::velox::connector::hive::delta
