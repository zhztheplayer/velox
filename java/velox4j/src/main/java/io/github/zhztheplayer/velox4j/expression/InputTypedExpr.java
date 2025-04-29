package io.github.zhztheplayer.velox4j.expression;

import java.util.Collections;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.github.zhztheplayer.velox4j.type.Type;

public class InputTypedExpr extends TypedExpr {
  @JsonCreator
  public InputTypedExpr(@JsonProperty("type") Type returnType) {
    super(returnType, Collections.emptyList());
  }
}
