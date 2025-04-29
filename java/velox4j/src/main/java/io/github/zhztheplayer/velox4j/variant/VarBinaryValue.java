package io.github.zhztheplayer.velox4j.variant;

import java.util.Base64;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class VarBinaryValue extends Variant {
  private final String base64;

  @JsonCreator
  public VarBinaryValue(@JsonProperty("value") String base64) {
    this.base64 = base64;
  }

  public static VarBinaryValue create(byte[] bytes) {
    return new VarBinaryValue(Base64.getEncoder().encodeToString(bytes));
  }

  @JsonGetter("value")
  public String getBase64() {
    return base64;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VarBinaryValue that = (VarBinaryValue) o;
    return Objects.equals(base64, that.base64);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(base64);
  }

  @Override
  public String toString() {
    return "VarBinaryValue{" + "base64='" + base64 + '\'' + '}';
  }
}
