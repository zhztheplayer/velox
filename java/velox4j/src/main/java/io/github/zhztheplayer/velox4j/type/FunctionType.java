package io.github.zhztheplayer.velox4j.type;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FunctionType extends Type {
  private final List<Type> children;

  @JsonCreator
  private FunctionType(@JsonProperty("cTypes") List<Type> children) {
    this.children = children;
  }

  public static FunctionType create(List<Type> argumentTypes, Type returnType) {
    final List<Type> mergedTypes = new ArrayList<>(argumentTypes);
    mergedTypes.add(returnType);
    return new FunctionType(mergedTypes);
  }

  @JsonGetter("cTypes")
  public List<Type> getChildren() {
    return children;
  }
}
