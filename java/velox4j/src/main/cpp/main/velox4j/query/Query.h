#pragma once

#include <velox/core/PlanNode.h>
#include "velox4j/conf/Config.h"

namespace velox4j {
class Query : public facebook::velox::ISerializable {
 public:
  Query(
      const std::shared_ptr<const facebook::velox::core::PlanNode>& plan,
      const std::shared_ptr<const ConfigArray>& queryConfig,
      const std::shared_ptr<const ConnectorConfigArray>& connectorConfig);

  const std::shared_ptr<const facebook::velox::core::PlanNode>& plan() const;
  const std::shared_ptr<const ConfigArray>& queryConfig() const;
  const std::shared_ptr<const ConnectorConfigArray>& connectorConfig() const;

  folly::dynamic serialize() const override;

  std::string toString() const;

  static std::shared_ptr<Query> create(
      const folly::dynamic& obj,
      void* context);

  static void registerSerDe();

 private:
  const std::shared_ptr<const facebook::velox::core::PlanNode> plan_;
  const std::shared_ptr<const ConfigArray> queryConfig_;
  const std::shared_ptr<const ConnectorConfigArray> connectorConfig_;
};
} // namespace velox4j
