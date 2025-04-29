package io.github.zhztheplayer.velox4j.variant;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class TimestampValue extends Variant {
  private static final int DUMMY_VALUE = -1;
  private final int value;
  private final long seconds;
  private final long nanos;

  @JsonCreator
  private TimestampValue(
      @JsonProperty("value") int value,
      @JsonProperty("seconds") long seconds,
      @JsonProperty("nanos") long nanos) {
    // JSON field "value" is always -1 be Velox's design.
    Preconditions.checkArgument(
        value == DUMMY_VALUE, "JSON field \"value\" has to be -1 in timestamp variant");
    this.value = value;
    this.seconds = seconds;
    this.nanos = nanos;
  }

  public static TimestampValue create(long seconds, long nanos) {
    return new TimestampValue(DUMMY_VALUE, seconds, nanos);
  }

  @JsonGetter("value")
  public int getValue() {
    return value;
  }

  @JsonGetter("seconds")
  public long getSeconds() {
    return seconds;
  }

  @JsonGetter("nanos")
  public long getNanos() {
    return nanos;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TimestampValue that = (TimestampValue) o;
    return value == that.value && seconds == that.seconds && nanos == that.nanos;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, seconds, nanos);
  }

  @Override
  public String toString() {
    return "TimestampValue{" + "value=" + value + ", seconds=" + seconds + ", nanos=" + nanos + '}';
  }
}
