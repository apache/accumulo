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
package org.apache.accumulo.core.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import org.apache.commons.configuration2.CompositeConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeprecatedPropertyUtilTest {
  private static class TestPropertyUtil extends DeprecatedPropertyUtil {
    public static void registerTestRenamer() {
      renamers.add(new PropertyRenamer() {
        private static final String OLD_PREFIX = "old.";

        @Override
        public boolean matches(String property) {
          return property.startsWith(OLD_PREFIX);
        }

        @Override
        public String rename(String property) {
          return "new." + property.substring(OLD_PREFIX.length());
        }
      });
    }
  }

  @BeforeClass
  public static void setup() {
    TestPropertyUtil.registerTestRenamer();
  }

  @Test
  public void testNonDeprecatedPropertyRename() {
    String oldProp = "some_property_name";
    String newProp = DeprecatedPropertyUtil.renameDeprecatedProperty(oldProp);
    assertEquals(oldProp, newProp);
    assertSame(oldProp, newProp);
  }

  @Test
  public void testDeprecatedPropertyRename() {
    String newProp = DeprecatedPropertyUtil.renameDeprecatedProperty("old.test", false);
    assertEquals("new.test", newProp);
  }

  @Test
  public void testSanityCheckWithOldProp() {
    CompositeConfiguration config = new CompositeConfiguration();
    config.setProperty("old.prop", "3");
    DeprecatedPropertyUtil.sanityCheck(config);
  }

  @Test
  public void testSanityCheckWithNewProp() {
    CompositeConfiguration config = new CompositeConfiguration();
    config.setProperty("new.prop", "4");
    DeprecatedPropertyUtil.sanityCheck(config);
  }

  @Test
  public void testSanityCheckFailure() {
    CompositeConfiguration config = new CompositeConfiguration();
    config.setProperty("old.prop", "3");
    config.setProperty("new.prop", "4");
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> DeprecatedPropertyUtil.sanityCheck(config));
    assertEquals("new.prop and deprecated old.prop cannot both be site in the configuration.",
        e.getMessage());
  }
}
