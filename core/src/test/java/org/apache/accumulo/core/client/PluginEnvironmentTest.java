package org.apache.accumulo.core.client;

import static org.apache.accumulo.core.client.PluginEnvironment.Configuration;
import static org.apache.accumulo.core.conf.Property.TABLE_MAJC_RATIO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

public class PluginEnvironmentTest {

  @Test
  public void testFromWithoutDefaultsContainsOnlySuppliedProps() {
    Map<String,String> props = new HashMap<>();
    props.put("table.custom.foo", "true");
    props.put("table.custom.bar", "false");

    Configuration config = Configuration.from(props, false);

    assertEquals("true", config.get("table.custom.foo"));
    assertEquals("false", config.get("table.custom.bar"));

    assertNull(config.get(TABLE_MAJC_RATIO.getKey()));
    assertNull(config.get("table.custom.test.missing"));
  }

  @Test
  public void testFromWithDefaults() {
    Map<String,String> props = new HashMap<>();
    props.put("table.custom.foo", "bar");

    Configuration config = Configuration.from(props, true);
    assertEquals("bar", config.get("table.custom.foo"));
    assertEquals(TABLE_MAJC_RATIO.getDefaultValue(), config.get(TABLE_MAJC_RATIO.getKey()));
  }

  /**
   * The supplied map must override the defaults values on collision.
   */
  @Test
  public void testSuppliedPropertiesOverrideDefaults() {
    String key = TABLE_MAJC_RATIO.getKey();
    String defaultValue = TABLE_MAJC_RATIO.getDefaultValue();
    String overrideValue = "10";
    assertEquals(overrideValue, defaultValue, "default value cannot match override value");
    Configuration config = Configuration.from(Map.of(key, overrideValue), true);
    assertEquals(overrideValue, config.get(key));
  }

  @Test
  public void testFromEmptyMapWithoutDefaultsIsEmpty() {
    Configuration config = Configuration.from(Map.of(), false);

    assertNull(config.get("table.custom.test"));
    assertNull(config.get(TABLE_MAJC_RATIO.getKey()));
    assertFalse(config.iterator().hasNext(),
        "iterator should be empty when no props and no defaults");
  }

  /**
   * getDerived should produce a Supplier that computes its value from the underlying configuration.
   */
  @Test
  public void testGetDerived() {
    Configuration config = Configuration.from(Map.of("table.custom.test", "5"), false);

    Supplier<Integer> derived =
        config.getDerived(cfg -> Integer.parseInt(cfg.get("table.custom.test")));

    assertNotNull(derived);
    assertEquals(5, derived.get());
  }
}
