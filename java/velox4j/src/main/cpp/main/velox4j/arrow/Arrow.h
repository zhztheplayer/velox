#pragma once

#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <velox/vector/ComplexVector.h>

namespace velox4j {
void fromBaseVectorToArrow(
    facebook::velox::VectorPtr vector,
    ArrowSchema* cSchema,
    ArrowArray* cArray);

facebook::velox::VectorPtr fromArrowToBaseVector(
    facebook::velox::memory::MemoryPool* pool,
    ArrowSchema* cSchema,
    ArrowArray* cArray);
} // namespace velox4j
