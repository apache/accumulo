/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.DeprecatedPropertyUtil;
import org.apache.accumulo.core.conf.DeprecatedPropertyUtil.PropertyRenamer;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.util.SystemPropUtil;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

  @Before
  public void setUpRenamers() throws Exception {
    super.setUp();
    TestPropertyUtil.registerTestRenamer();
  }

  @After
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
      assertFalse(oldProp + " was in the config!", config.containsKey(newProp));
      assertFalse(newProp + " was in the config!", config.containsKey(newProp));

      // create using old prop and verify new prop was created
      SystemPropUtil.setSystemProperty(getServerContext(), oldProp, propValue);
      config = client.instanceOperations().getSystemConfiguration();
      assertFalse(oldProp + " was in the config after set call!", config.containsKey(oldProp));
      assertTrue(newProp + " was not in the config after set call!", config.containsKey(newProp));
      assertEquals(propValue, config.get(newProp));

      // remove using new prop and verify both are gone
      SystemPropUtil.removeSystemProperty(getServerContext(), newProp);
      config = client.instanceOperations().getSystemConfiguration();
      assertFalse(oldProp + " was in the config after remove call!", config.containsKey(oldProp));
      assertFalse(newProp + " was in the config after remove call!", config.containsKey(newProp));

      // re-create using new prop and verify new prop was created
      SystemPropUtil.setSystemProperty(getServerContext(), newProp, propValue);
      config = client.instanceOperations().getSystemConfiguration();
      assertFalse(oldProp + " was in the config after set call!", config.containsKey(oldProp));
      assertTrue(newProp + " was not in the config after set call!", config.containsKey(newProp));
      assertEquals(propValue, config.get(newProp));

      // remove using old prop and verify both are gone
      SystemPropUtil.removeSystemProperty(getServerContext(), oldProp);
      config = client.instanceOperations().getSystemConfiguration();
      assertFalse(oldProp + " was in the config after remove call!", config.containsKey(oldProp));
      assertFalse(newProp + " was in the config after remove call!", config.containsKey(newProp));
    }
  }

}
