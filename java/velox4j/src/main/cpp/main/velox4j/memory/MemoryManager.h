#pragma once

#include <arrow/memory_pool.h>
#include <velox/common/config/Config.h>
#include <velox/common/memory/Memory.h>
#include <memory>
#include "AllocationListener.h"
#include "ArrowMemoryPool.h"

namespace velox4j {

namespace {}

class MemoryManager {
 public:
  explicit MemoryManager(std::unique_ptr<AllocationListener> listener);

  virtual ~MemoryManager();

  MemoryManager(const MemoryManager&) = delete;
  MemoryManager(MemoryManager&&) = delete;
  MemoryManager& operator=(const MemoryManager&) = delete;
  MemoryManager& operator=(MemoryManager&&) = delete;

  facebook::velox::memory::MemoryPool* getVeloxPool(
      const std::string& name,
      const facebook::velox::memory::MemoryPool::Kind& kind);

  arrow::MemoryPool* getArrowPool(const std::string& name);

 private:
  bool tryDestruct();

  const std::unique_ptr<AllocationListener> listener_;
  std::unique_ptr<MemoryAllocator> arrowAllocator_;
  std::unordered_map<std::string, std::unique_ptr<arrow::MemoryPool>>
      arrowPoolRefs_;
  std::unique_ptr<facebook::velox::memory::MemoryManager> veloxMemoryManager_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxRootPool_;
  std::unordered_map<
      std::string,
      std::shared_ptr<facebook::velox::memory::MemoryPool>>
      veloxPoolRefs_;
};
} // namespace velox4j
