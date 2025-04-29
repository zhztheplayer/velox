#pragma once

#include <velox/expression/Expr.h>
#include "velox4j/conf/Config.h"

namespace velox4j {
class Evaluation : public facebook::velox::ISerializable {
 public:
  Evaluation(
      const facebook::velox::core::TypedExprPtr& expr,
      const std::shared_ptr<const ConfigArray>& queryConfig,
      const std::shared_ptr<const ConnectorConfigArray>& connectorConfig);

  const facebook::velox::core::TypedExprPtr& expr() const;
  const std::shared_ptr<const ConfigArray>& queryConfig() const;
  const std::shared_ptr<const ConnectorConfigArray>& connectorConfig() const;

  folly::dynamic serialize() const override;

  static std::shared_ptr<Evaluation> create(
      const folly::dynamic& obj,
      void* context);

  static void registerSerDe();

 private:
  const facebook::velox::core::TypedExprPtr expr_;
  const std::shared_ptr<const ConfigArray> queryConfig_;
  const std::shared_ptr<const ConnectorConfigArray> connectorConfig_;
};
} // namespace velox4j
