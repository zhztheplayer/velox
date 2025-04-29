#pragma once

#include <JniHelpers.h>
#include <atomic>
#include "velox4j/memory/AllocationListener.h"

namespace velox4j {
class JavaAllocationListenerJniWrapper final : public spotify::jni::JavaClass {
 public:
  explicit JavaAllocationListenerJniWrapper(JNIEnv* env) : JavaClass(env) {
    JavaAllocationListenerJniWrapper::initialize(env);
  }

  JavaAllocationListenerJniWrapper() : JavaClass(){};

  const char* getCanonicalName() const override;

  void initialize(JNIEnv* env) override;

  void mapFields() override;
};

class JavaAllocationListener : public AllocationListener {
 public:
  // CTOR.
  JavaAllocationListener(JNIEnv* env, jobject ref);

  // DTOR.
  ~JavaAllocationListener() override;

  void allocationChanged(int64_t diff) override;
  const int64_t currentBytes() const override;
  const int64_t peakBytes() const override;

 private:
  jobject ref_;
  std::atomic_int64_t usedBytes_{0L};
  std::atomic_int64_t peakBytes_{0L};
};
} // namespace velox4j
