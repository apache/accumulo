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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ConcurrentModificationException;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.DeprecatedPropertyUtil;
import org.apache.accumulo.core.conf.DeprecatedPropertyUtil.PropertyRenamer;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.util.SystemPropUtil;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DeprecatedPropertyUtilIT extends ConfigurableMacBase {
  private static final String OLD_SYSTEM_PREFIX = "old.system.custom.";

  private static final PropertyRenamer TEST_SYS_RENAMER = PropertyRenamer
      .renamePrefix(OLD_SYSTEM_PREFIX, Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey());

  private static class TestPropertyUtil extends DeprecatedPropertyUtil {
    public static void registerTestRenamer() {
      renamers.add(TEST_SYS_RENAMER);
    }

    public static void removeTestRenamer() {
      renamers.remove(TEST_SYS_RENAMER);
    }
  }

  @BeforeEach
  public void setUpRenamers() throws Exception {
    super.setUp();
    TestPropertyUtil.registerTestRenamer();
  }

  @AfterEach
  public void tearDownRenamers() {
    super.tearDown();
    TestPropertyUtil.removeTestRenamer();
  }

  @Test
  public void testSystemProperty() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String oldProp = OLD_SYSTEM_PREFIX + "test.prop";
      String newProp = Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + "test.prop";
      String propValue = "dummy prop value";
      Map<String,String> config = client.instanceOperations().getSystemConfiguration();
      assertFalse(config.containsKey(newProp), oldProp + " was in the config!");
      assertFalse(config.containsKey(newProp), newProp + " was in the config!");

      // create using old prop and verify new prop was created
      SystemPropUtil.setSystemProperty(getServerContext(), oldProp, propValue);
      config = client.instanceOperations().getSystemConfiguration();
      assertFalse(config.containsKey(oldProp), oldProp + " was in the config after set call!");
      assertTrue(config.containsKey(newProp), newProp + " was not in the config after set call!");
      assertEquals(propValue, config.get(newProp));

      // remove using new prop and verify both are gone
      SystemPropUtil.removeSystemProperty(getServerContext(), newProp);
      config = client.instanceOperations().getSystemConfiguration();
      assertFalse(config.containsKey(oldProp), oldProp + " was in the config after remove call!");
      assertFalse(config.containsKey(newProp), newProp + " was in the config after remove call!");

      // re-create using new prop and verify new prop was created
      SystemPropUtil.setSystemProperty(getServerContext(), newProp, propValue);
      config = client.instanceOperations().getSystemConfiguration();
      assertFalse(config.containsKey(oldProp), oldProp + " was in the config after set call!");
      assertTrue(config.containsKey(newProp), newProp + " was not in the config after set call!");
      assertEquals(propValue, config.get(newProp));

      // remove using old prop and verify both are gone
      SystemPropUtil.removeSystemProperty(getServerContext(), oldProp);
      config = client.instanceOperations().getSystemConfiguration();
      assertFalse(config.containsKey(oldProp), oldProp + " was in the config after remove call!");
      assertFalse(config.containsKey(newProp), newProp + " was in the config after remove call!");
    }
  }

  @Test
  public void testModifyProperties() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String oldProp = OLD_SYSTEM_PREFIX + "test.prop";
      String newProp = Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + "test.prop";
      String propValue = "dummy prop value";
      Map<String,String> config = client.instanceOperations().getSystemConfiguration();
      assertFalse(config.containsKey(newProp), oldProp + " was in the config!");
      assertFalse(config.containsKey(newProp), newProp + " was in the config!");

      // create using old prop and verify new prop was created
      SystemPropUtil.modifyProperties(getServerContext(), 0, Map.of(oldProp, propValue));
      config = client.instanceOperations().getSystemConfiguration();
      assertFalse(config.containsKey(oldProp), oldProp + " was in the config after set call!");
      assertTrue(config.containsKey(newProp), newProp + " was not in the config after set call!");
      assertEquals(propValue, config.get(newProp));

      try {
        // test version detection
        SystemPropUtil.modifyProperties(getServerContext(), 0, Map.of());
        fail("Expected ConcurrentModificationException as version has changed");
      } catch (ConcurrentModificationException e) {
        // expected
      }

      // remove using new prop and verify both are gone
      SystemPropUtil.modifyProperties(getServerContext(), 1, Map.of());
      config = client.instanceOperations().getSystemConfiguration();
      assertFalse(config.containsKey(oldProp), oldProp + " was in the config after remove call!");
      assertFalse(config.containsKey(newProp), newProp + " was in the config after remove call!");

      // re-create using new prop and verify new prop was created
      SystemPropUtil.modifyProperties(getServerContext(), 2, Map.of(newProp, propValue));
      config = client.instanceOperations().getSystemConfiguration();
      assertFalse(config.containsKey(oldProp), oldProp + " was in the config after set call!");
      assertTrue(config.containsKey(newProp), newProp + " was not in the config after set call!");
      assertEquals(propValue, config.get(newProp));

      // remove using old prop and verify both are gone
      SystemPropUtil.modifyProperties(getServerContext(), 3, Map.of());
      config = client.instanceOperations().getSystemConfiguration();
      assertFalse(config.containsKey(oldProp), oldProp + " was in the config after remove call!");
      assertFalse(config.containsKey(newProp), newProp + " was in the config after remove call!");
    }
  }

}
