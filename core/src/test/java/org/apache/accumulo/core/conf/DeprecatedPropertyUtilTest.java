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

import java.util.function.BiConsumer;

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

}
