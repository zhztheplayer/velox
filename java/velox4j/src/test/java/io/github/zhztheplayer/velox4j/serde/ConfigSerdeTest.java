package io.github.zhztheplayer.velox4j.serde;

import org.junit.BeforeClass;
import org.junit.Test;

import io.github.zhztheplayer.velox4j.conf.Config;
import io.github.zhztheplayer.velox4j.conf.ConnectorConfig;
import io.github.zhztheplayer.velox4j.test.ConfigTests;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;

public class ConfigSerdeTest {
  @BeforeClass
  public static void beforeClass() {
    Velox4jTests.ensureInitialized();
  }

  @Test
  public void testConfig() {
    final Config config = ConfigTests.randomConfig();
    SerdeTests.testISerializableRoundTrip(config);
  }

  @Test
  public void testConnectorConfig() {
    final ConnectorConfig connConfig = ConfigTests.randomConnectorConfig();
    SerdeTests.testISerializableRoundTrip(connConfig);
  }
}
