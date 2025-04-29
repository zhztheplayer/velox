#pragma once

#include <velox/common/serialization/Serializable.h>
#include <velox/core/QueryConfig.h>

namespace velox4j {
class ConfigArray : public facebook::velox::ISerializable {
 public:
  explicit ConfigArray(
      std::vector<std::pair<std::string, std::string>>&& values)
      : values_(std::move(values)) {}

  std::unordered_map<std::string, std::string> toMap() const;

  folly::dynamic serialize() const override;

  static std::shared_ptr<ConfigArray> empty();

  static std::shared_ptr<ConfigArray> create(const folly::dynamic& obj);

  static void registerSerDe();

 private:
  const std::vector<std::pair<std::string, std::string>> values_;
};

class ConnectorConfigArray : public facebook::velox::ISerializable {
 public:
  explicit ConnectorConfigArray(
      std::vector<std::pair<std::string, std::shared_ptr<const ConfigArray>>>&&
          values)
      : values_(std::move(values)) {}

  std::unordered_map<
      std::string,
      std::shared_ptr<facebook::velox::config::ConfigBase>>
  toMap() const;

  folly::dynamic serialize() const override;

  static std::shared_ptr<ConnectorConfigArray> create(
      const folly::dynamic& obj);

  static void registerSerDe();

 private:
  const std::vector<std::pair<std::string, std::shared_ptr<const ConfigArray>>>
      values_;
};
} // namespace velox4j
