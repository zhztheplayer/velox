package io.github.zhztheplayer.velox4j.serde;

import java.util.Collections;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.github.zhztheplayer.velox4j.conf.Config;
import io.github.zhztheplayer.velox4j.conf.ConnectorConfig;
import io.github.zhztheplayer.velox4j.eval.Evaluation;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import io.github.zhztheplayer.velox4j.type.IntegerType;

public class EvaluationSerdeTest {
  private static MemoryManager memoryManager;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
    memoryManager = MemoryManager.create(AllocationListener.NOOP);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    memoryManager.close();
  }

  @Test
  public void testExpression() {
    final CallTypedExpr expr =
        new CallTypedExpr(new IntegerType(), Collections.emptyList(), "random_int");
    final Evaluation evaluation = new Evaluation(expr, Config.empty(), ConnectorConfig.empty());
    SerdeTests.testISerializableRoundTrip(evaluation);
  }
}
