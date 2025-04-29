#pragma once

#include <JniHelpers.h>

namespace velox4j {
class JniWrapper final : public spotify::jni::JavaClass {
 public:
  explicit JniWrapper(JNIEnv* env) : JavaClass(env) {
    JniWrapper::initialize(env);
  }

  const char* getCanonicalName() const override;

  void initialize(JNIEnv* env) override;

  void mapFields() override;
};
} // namespace velox4j
