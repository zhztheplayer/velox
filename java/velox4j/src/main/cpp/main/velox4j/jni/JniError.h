#pragma once

#include <stdexcept>

#include "JniCommon.h"

namespace velox4j {

class JniErrorState {
 public:
  virtual ~JniErrorState() = default;

  void ensureInitialized(JNIEnv* env);

  void assertInitialized() const;

  void close();

  jclass runtimeExceptionClass();

  jclass illegalAccessExceptionClass();

  jclass veloxExceptionClass();

 private:
  void initialize(JNIEnv* env);

  jclass ioExceptionClass_ = nullptr;
  jclass runtimeExceptionClass_ = nullptr;
  jclass unsupportedOperationExceptionClass_ = nullptr;
  jclass illegalAccessExceptionClass_ = nullptr;
  jclass illegalArgumentExceptionClass_ = nullptr;
  jclass veloxExceptionClass_ = nullptr;
  JavaVM* vm_;
  bool initialized_{false};
  bool closed_{false};
  std::mutex mtx_;
};

inline JniErrorState* getJniErrorState() {
  static JniErrorState jniErrorState;
  return &jniErrorState;
}

} // namespace velox4j
