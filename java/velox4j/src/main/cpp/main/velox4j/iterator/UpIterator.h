#pragma once

#include <velox/vector/ComplexVector.h>
#include "velox4j/query/Query.h"

namespace velox4j {

class UpIterator {
 public:
  enum class State { AVAILABLE = 0, BLOCKED = 1, FINISHED = 2 };

  // CTOR.
  UpIterator() = default;

  // Delete copy/move CTORs.
  UpIterator(UpIterator&&) = delete;
  UpIterator(const UpIterator&) = delete;
  UpIterator& operator=(const UpIterator&) = delete;
  UpIterator& operator=(UpIterator&&) = delete;

  // DTOR.
  virtual ~UpIterator() = default;

  // Iteration control.
  virtual State advance() = 0;
  virtual void wait() = 0;
  virtual facebook::velox::RowVectorPtr get() = 0;
};
} // namespace velox4j
