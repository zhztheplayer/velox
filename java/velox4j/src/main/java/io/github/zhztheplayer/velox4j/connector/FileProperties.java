package io.github.zhztheplayer.velox4j.connector;

import java.util.OptionalLong;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FileProperties {
  private final OptionalLong fileSize;
  private final OptionalLong modificationTime;

  @JsonCreator
  public FileProperties(
      @JsonProperty("fileSize") OptionalLong fileSize,
      @JsonProperty("modificationTime") OptionalLong modificationTime) {
    this.fileSize = fileSize;
    this.modificationTime = modificationTime;
  }

  @JsonGetter("fileSize")
  public OptionalLong getFileSize() {
    return fileSize;
  }

  @JsonGetter("modificationTime")
  public OptionalLong getModificationTime() {
    return modificationTime;
  }
}
