#pragma once

#include "velox4j/init/Config.h"
#include "velox4j/init/Init.h"

namespace velox4j {
inline void testingEnsureInitializedForSpark() {
  static std::once_flag flag;
  auto conf = std::make_shared<ConfigArray>(
      std::vector<std::pair<std::string, std::string>>{
          {VELOX4J_INIT_PRESET.key, folly::to<std::string>(Preset::SPARK)}});
  std::call_once(flag, [&]() { initialize(conf); });
}
} // namespace velox4j
