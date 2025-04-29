#pragma once

#include <JniHelpers.h>

namespace velox4j {
class StaticJniWrapper final : public spotify::jni::JavaClass {
 public:
  explicit StaticJniWrapper(JNIEnv* env) : JavaClass(env) {
    StaticJniWrapper::initialize(env);
  }

  const char* getCanonicalName() const override;

  void initialize(JNIEnv* env) override;

  void mapFields() override;
};
} // namespace velox4j
