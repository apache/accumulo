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
package org.apache.accumulo.core.util.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.DateFormat;

import org.junit.jupiter.api.Test;

public class FormatterConfigTest {

  @Test
  public void testConstructor() {
    FormatterConfig config = new FormatterConfig();
    assertFalse(config.willLimitShowLength());
    assertFalse(config.willPrintTimestamps());
  }

  @Test
  public void testSetShownLength() {
    FormatterConfig config = new FormatterConfig();
    assertThrows(IllegalArgumentException.class, () -> config.setShownLength(-1),
        "Should throw on negative length.");

    config.setShownLength(0);
    assertEquals(0, config.getShownLength());
    assertTrue(config.willLimitShowLength());

    config.setShownLength(1);
    assertEquals(1, config.getShownLength());
    assertTrue(config.willLimitShowLength());
  }

  @Test
  public void testDoNotLimitShowLength() {
    FormatterConfig config = new FormatterConfig();
    assertFalse(config.willLimitShowLength());

    config.setShownLength(1);
    assertTrue(config.willLimitShowLength());

    config.doNotLimitShowLength();
    assertFalse(config.willLimitShowLength());
  }

  @Test
  public void testGetDateFormat() {
    FormatterConfig config1 = new FormatterConfig();
    DateFormat df1 = config1.getDateFormatSupplier().get();

    FormatterConfig config2 = new FormatterConfig();
    assertNotSame(df1, config2.getDateFormatSupplier().get());

    config2.setDateFormatSupplier(config1.getDateFormatSupplier());
    assertSame(df1, config2.getDateFormatSupplier().get());

    // even though copying, it can't copy the Generator, so will pull out the same DateFormat
    FormatterConfig configCopy = new FormatterConfig(config1);
    assertSame(df1, configCopy.getDateFormatSupplier().get());
  }

}
