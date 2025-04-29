package io.github.zhztheplayer.velox4j.exception;

public class VeloxException extends RuntimeException {
  public VeloxException() {
    super();
  }

  public VeloxException(String message) {
    super(message);
  }

  public VeloxException(String message, Throwable cause) {
    super(message, cause);
  }

  public VeloxException(Throwable cause) {
    super(cause);
  }

  protected VeloxException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
