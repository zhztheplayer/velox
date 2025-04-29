#pragma once

#include <velox/common/config/Config.h>

namespace velox4j {
enum Preset { SPARK = 0 };

extern facebook::velox::config::ConfigBase::Entry<Preset> VELOX4J_INIT_PRESET;
} // namespace velox4j
