package io.github.zhztheplayer.velox4j.variant;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TinyIntValue extends Variant {
  private final int value;

  @JsonCreator
  public TinyIntValue(@JsonProperty("value") int value) {
    this.value = value;
  }

  @JsonGetter("value")
  public int getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TinyIntValue that = (TinyIntValue) o;
    return value == that.value;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

  @Override
  public String toString() {
    return "TinyIntValue{" + "value=" + value + '}';
  }
}
