package io.github.zhztheplayer.velox4j.data;

import io.github.zhztheplayer.velox4j.jni.JniApi;

public class SelectivityVectors {
  private final JniApi jniApi;

  public SelectivityVectors(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public SelectivityVector create(int length) {
    return jniApi.createSelectivityVector(length);
  }
}
