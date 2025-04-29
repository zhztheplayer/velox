#pragma once

#include <memory>
#include "ObjectStore.h"
#include "velox4j/memory/MemoryManager.h"

namespace velox4j {
class Session {
 public:
  Session(MemoryManager* memoryManager)
      : memoryManager_(memoryManager), objectStore_(ObjectStore::create()){};
  virtual ~Session() = default;

  MemoryManager* memoryManager() {
    return memoryManager_;
  }

  ObjectStore* objectStore() {
    return objectStore_.get();
  }

 private:
  MemoryManager* memoryManager_;
  std::unique_ptr<ObjectStore> objectStore_;
};

} // namespace velox4j
