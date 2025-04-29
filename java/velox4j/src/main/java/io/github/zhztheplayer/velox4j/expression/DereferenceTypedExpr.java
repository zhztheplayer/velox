package io.github.zhztheplayer.velox4j.expression;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;

public class DereferenceTypedExpr extends TypedExpr {
  private final int index;

  @JsonCreator
  private DereferenceTypedExpr(
      @JsonProperty("type") Type returnType,
      @JsonProperty("inputs") List<TypedExpr> inputs,
      @JsonProperty("fieldIndex") int index) {
    super(returnType, inputs);
    this.index = index;
    Preconditions.checkArgument(
        inputs.size() == 1, "DereferenceTypedExpr should have 1 input, but has %s", inputs.size());
    Preconditions.checkArgument(
        inputs.get(0).getReturnType() instanceof RowType,
        "DereferenceTypedExpr input should be RowType");
  }

  public static DereferenceTypedExpr create(TypedExpr input, int index) {
    final Type inputType = input.getReturnType();
    Preconditions.checkArgument(
        inputType instanceof RowType, "DereferenceTypedExpr input should be RowType");
    final Type returnType = ((RowType) inputType).getChildren().get(index);
    return new DereferenceTypedExpr(returnType, Collections.singletonList(input), index);
  }

  @JsonGetter("fieldIndex")
  public int getIndex() {
    return index;
  }
}
