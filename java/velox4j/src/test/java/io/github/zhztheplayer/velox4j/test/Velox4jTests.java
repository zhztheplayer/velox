package io.github.zhztheplayer.velox4j.test;

import java.util.concurrent.atomic.AtomicBoolean;

import io.github.zhztheplayer.velox4j.Velox4j;

public class Velox4jTests {
  private static final AtomicBoolean initialized = new AtomicBoolean(false);

  public static void ensureInitialized() {
    if (!initialized.compareAndSet(false, true)) {
      return;
    }
    Velox4j.initialize();
  }
}
