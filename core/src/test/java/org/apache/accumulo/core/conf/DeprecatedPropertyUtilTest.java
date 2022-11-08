/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.conf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.function.BiConsumer;

import org.apache.commons.configuration2.BaseConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

public class DeprecatedPropertyUtilTest {

  private static class TestPropertyUtil extends DeprecatedPropertyUtil {
    private static final String OLD_PREFIX = "old.";
    private static final String MIDDLE_PREFIX = "middle.";
    private static final String NEW_PREFIX = "new.";

    public static void registerTestRenamer() {
      renamers.add(PropertyRenamer.renamePrefix(OLD_PREFIX, MIDDLE_PREFIX));
      renamers.add(PropertyRenamer.renamePrefix(MIDDLE_PREFIX, NEW_PREFIX));
    }
  }

  private static final BiConsumer<Logger,String> NOOP = (log, replacement) -> {};

  @BeforeAll
  public static void setup() {
    TestPropertyUtil.registerTestRenamer();
  }

  @Test
  public void testNonDeprecatedPropertyRename() {
    String oldProp = "some_property_name";
    String newProp = DeprecatedPropertyUtil.getReplacementName(oldProp, NOOP);
    assertSame(oldProp, newProp);
  }

  @Test
  public void testDeprecatedPropertyRename() {
    // 'middle.test' -> 'new.test'
    String newProp = DeprecatedPropertyUtil.getReplacementName("middle.test", NOOP);
    assertEquals("new.test", newProp);
    // 'old.test' -> 'middle.test' -> 'new.test'
    String newProp2 = DeprecatedPropertyUtil.getReplacementName("old.test", NOOP);
    assertEquals("new.test", newProp2);
  }

  @Test
  public void testMasterManagerPropertyRename() {
    Arrays.stream(Property.values()).filter(p -> p.getType() != PropertyType.PREFIX)
        .filter(p -> p.getKey().startsWith(Property.MANAGER_PREFIX.getKey())).forEach(p -> {
          String oldProp =
              "master." + p.getKey().substring(Property.MANAGER_PREFIX.getKey().length());
          assertEquals(p.getKey(), DeprecatedPropertyUtil.getReplacementName(oldProp, NOOP));
        });
  }

  @Test
  public void testSanityCheckManagerProperties() {
    var config = new BaseConfiguration();
    config.setProperty("regular.prop1", "value");
    config.setProperty("regular.prop2", "value");
    assertEquals(2, config.size());
    DeprecatedPropertyUtil.sanityCheckManagerProperties(config); // should succeed
    config.setProperty("master.deprecatedProp", "value");
    assertEquals(3, config.size());
    DeprecatedPropertyUtil.sanityCheckManagerProperties(config); // should succeed
    config.setProperty("manager.replacementProp", "value");
    assertEquals(4, config.size());
    assertThrows(IllegalStateException.class,
        () -> DeprecatedPropertyUtil.sanityCheckManagerProperties(config),
        "Sanity check should fail when 'master.*' and 'manager.*' appear in same config");
    config.clearProperty("master.deprecatedProp");
    assertEquals(3, config.size());
    DeprecatedPropertyUtil.sanityCheckManagerProperties(config); // should succeed
  }

}
