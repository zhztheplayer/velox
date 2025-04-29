package io.github.zhztheplayer.velox4j.variant;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BigIntValue extends Variant {
  private final long value;

  @JsonCreator
  public BigIntValue(@JsonProperty("value") long value) {
    this.value = value;
  }

  @JsonGetter("value")
  public long getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BigIntValue that = (BigIntValue) o;
    return value == that.value;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

  @Override
  public String toString() {
    return "BigIntValue{" + "value=" + value + '}';
  }
}
