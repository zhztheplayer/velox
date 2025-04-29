package io.github.zhztheplayer.velox4j.test;

import java.util.List;

import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;

public final class SampleQueryTests {
  private static final String SAMPLE_QUERY_PATH = "query/example-1.json";
  private static final String SAMPLE_QUERY_OUTPUT_PATH = "query-output/example-1.tsv";
  private static final RowType SAMPLE_QUERY_TYPE =
      new RowType(
          List.of("c0", "a0", "a1"), List.of(new BigIntType(), new BigIntType(), new BigIntType()));

  public static RowType getSchema() {
    return SAMPLE_QUERY_TYPE;
  }

  public static String readQueryJson() {
    return ResourceTests.readResourceAsString(SAMPLE_QUERY_PATH);
  }

  public static void assertIterator(UpIterator itr) {
    assertIterator(itr, 1);
  }

  public static void assertIterator(UpIterator itr, int repeatTimes) {
    UpIteratorTests.IteratorAssertionBuilder builder =
        UpIteratorTests.assertIterator(itr).assertNumRowVectors(repeatTimes);
    for (int i = 0; i < repeatTimes; i++) {
      builder =
          builder.assertRowVectorToString(
              i, ResourceTests.readResourceAsString(SAMPLE_QUERY_OUTPUT_PATH));
    }
    builder.run();
  }
}
