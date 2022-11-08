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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class DateStringFormatterTest {
  DateStringFormatter formatter;

  Map<Key,Value> data;

  @BeforeEach
  public void setUp() {
    formatter = new DateStringFormatter();
    data = new TreeMap<>();
    data.put(new Key("", "", "", 0), new Value());
  }

  private void testFormatterIgnoresConfig(FormatterConfig config, DateStringFormatter formatter) {
    // ignores config's DateFormatSupplier and substitutes its own
    formatter.initialize(data.entrySet(), config);

    assertTrue(formatter.hasNext());
    final String next = formatter.next();
    assertTrue(next.endsWith("1970/01/01 00:00:00.000"), next);
  }

  @Test
  public void testTimestamps() {
    final TimeZone utc = TimeZone.getTimeZone("UTC");
    final TimeZone est = TimeZone.getTimeZone("EST");
    final FormatterConfig config = new FormatterConfig().setPrintTimestamps(true);
    DateStringFormatter formatter;

    formatter = new DateStringFormatter(utc);
    testFormatterIgnoresConfig(config, formatter);

    // even though config says to use EST and only print year, the Formatter will override these
    formatter = new DateStringFormatter(utc);
    DateFormatSupplier dfSupplier = DateFormatSupplier.createSimpleFormatSupplier("YYYY", est);
    config.setDateFormatSupplier(dfSupplier);
    testFormatterIgnoresConfig(config, formatter);
  }

  @Test
  public void testNoTimestamps() {
    data.put(new Key("", "", "", 1), new Value());

    assertEquals(2, data.size());

    formatter.initialize(data.entrySet(), new FormatterConfig());

    assertEquals(formatter.next(), formatter.next());
  }

}
