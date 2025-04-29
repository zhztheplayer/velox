#pragma once

#include "velox4j/conf/Config.h"

namespace velox4j {
void initialize(const std::shared_ptr<ConfigArray>& configArray);
} // namespace velox4j
