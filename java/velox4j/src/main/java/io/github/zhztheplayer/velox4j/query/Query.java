package io.github.zhztheplayer.velox4j.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.github.zhztheplayer.velox4j.conf.Config;
import io.github.zhztheplayer.velox4j.conf.ConnectorConfig;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.serializable.ISerializable;

public class Query extends ISerializable {
  private final PlanNode plan;
  private final Config queryConfig;
  private final ConnectorConfig connectorConfig;

  @JsonCreator
  public Query(
      @JsonProperty("plan") PlanNode plan,
      @JsonProperty("queryConfig") Config queryConfig,
      @JsonProperty("connectorConfig") ConnectorConfig connectorConfig) {
    this.plan = plan;
    this.queryConfig = queryConfig;
    this.connectorConfig = connectorConfig;
  }

  @JsonGetter("plan")
  public PlanNode getPlan() {
    return plan;
  }

  @JsonGetter("queryConfig")
  public Config getQueryConfig() {
    return queryConfig;
  }

  @JsonGetter("connectorConfig")
  public ConnectorConfig getConnectorConfig() {
    return connectorConfig;
  }
}
