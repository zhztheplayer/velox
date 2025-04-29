package io.github.zhztheplayer.velox4j.exception;

import java.io.PrintWriter;
import java.io.StringWriter;

import io.github.zhztheplayer.velox4j.jni.CalledFromNative;

public class ExceptionDescriber {
  private ExceptionDescriber() {}

  @CalledFromNative
  static String describe(Throwable throwable) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    throwable.printStackTrace(pw);
    return sw.getBuffer().toString();
  }
}
