/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.util.format;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class DefaultFormatterTest {

  public static final TimeZone UTC = TimeZone.getTimeZone("UTC");
  public static final TimeZone EST = TimeZone.getTimeZone("EST");
  DefaultFormatter df;
  Iterable<Entry<Key,Value>> empty = Collections.<Key,Value> emptyMap().entrySet();

  @Before
  public void setUp() {
    df = new DefaultFormatter();
  }

  @Test(expected = IllegalStateException.class)
  public void testDoubleInitialize() {
    final FormatterConfig timestampConfig = new FormatterConfig().setPrintTimestamps(true);
    df.initialize(empty, timestampConfig);
    df.initialize(empty, timestampConfig);
  }

  @Test(expected = IllegalStateException.class)
  public void testNextBeforeInitialize() {
    df.hasNext();
  }

  @Test
  public void testAppendBytes() {
    StringBuilder sb = new StringBuilder();
    byte[] data = new byte[] {0, '\\', 'x', -0x01};

    DefaultFormatter.appendValue(sb, new Value());
    assertEquals("", sb.toString());

    DefaultFormatter.appendText(sb, new Text(data));
    assertEquals("\\x00\\\\x\\xFF", sb.toString());
  }

  @Test
  public void testFormatEntry() {
    final long timestamp = 0;
    Map<Key,Value> map = new TreeMap<>();
    map.put(new Key("a", "ab", "abc", timestamp), new Value("abcd".getBytes()));

    FormatterConfig config;
    String answer;

    // no timestamp, no max
    config = new FormatterConfig();
    df = new DefaultFormatter();
    df.initialize(map.entrySet(), config);
    answer = df.next();
    assertEquals("a ab:abc []\tabcd", answer);

    // yes timestamp, no max
    config.setPrintTimestamps(true);
    df = new DefaultFormatter();
    df.initialize(map.entrySet(), config);
    answer = df.next();
    assertEquals("a ab:abc [] " + timestamp + "\tabcd", answer);

    // yes timestamp, max of 1
    config.setPrintTimestamps(true).setShownLength(1);
    df = new DefaultFormatter();
    df.initialize(map.entrySet(), config);
    answer = df.next();
    assertEquals("a a:a [] " + timestamp + "\ta", answer);

    // yes timestamp, no max, new DateFormat
    config.setPrintTimestamps(true).doNotLimitShowLength().setDateFormatSupplier(DateFormatSupplier.createSimpleFormatSupplier("YYYY"));
    df = new DefaultFormatter();
    df.initialize(map.entrySet(), config);
    answer = df.next();
    assertEquals("a ab:abc [] 1970\tabcd", answer);

    // yes timestamp, no max, new DateFormat, different TimeZone
    config.setPrintTimestamps(true).doNotLimitShowLength().setDateFormatSupplier(DateFormatSupplier.createSimpleFormatSupplier("HH", UTC));
    df = new DefaultFormatter();
    df.initialize(map.entrySet(), config);
    answer = df.next();
    assertEquals("a ab:abc [] 00\tabcd", answer);

    config.setPrintTimestamps(true).doNotLimitShowLength().setDateFormatSupplier(DateFormatSupplier.createSimpleFormatSupplier("HH", EST));
    df = new DefaultFormatter();
    df.initialize(map.entrySet(), config);
    answer = df.next();
    assertEquals("a ab:abc [] 19\tabcd", answer);
  }
}
