package io.github.zhztheplayer.velox4j.eval;

import io.github.zhztheplayer.velox4j.jni.JniApi;

public class Evaluations {
  private final JniApi jniApi;

  public Evaluations(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public Evaluator createEvaluator(Evaluation evaluation) {
    return jniApi.createEvaluator(evaluation);
  }
}
