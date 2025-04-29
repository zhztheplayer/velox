#pragma once

#include <velox/common/memory/Memory.h>
#include <velox/expression/Expr.h>
#include <velox/vector/BaseVector.h>
#include <velox/vector/ComplexVector.h>
#include "velox4j/memory/MemoryManager.h"

namespace velox4j {
class Evaluator {
 public:
  Evaluator(MemoryManager* memoryManager, std::string exprJson);

  facebook::velox::VectorPtr eval(
      const facebook::velox::SelectivityVector& rows,
      const facebook::velox::RowVector& input);

 private:
  MemoryManager* const memoryManager_;
  const std::string exprJson_;
  std::shared_ptr<facebook::velox::core::QueryCtx> queryCtx_;
  std::unique_ptr<facebook::velox::core::ExpressionEvaluator> ee_;
  std::unique_ptr<facebook::velox::exec::ExprSet> exprSet_;
};
} // namespace velox4j
