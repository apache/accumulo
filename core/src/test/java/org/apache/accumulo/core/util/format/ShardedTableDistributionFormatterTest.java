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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.junit.Before;
import org.junit.Test;

public class ShardedTableDistributionFormatterTest {
  ShardedTableDistributionFormatter formatter;

  Map<Key,Value> data;

  @Before
  public void setUp() {
    data = new TreeMap<>();
    formatter = new ShardedTableDistributionFormatter();
  }

  @Test
  public void testInitialize() {
    data.put(new Key(), new Value());
    data.put(new Key("r", "~tab"), new Value());
    formatter.initialize(data.entrySet(), new FormatterConfig());

    assertTrue(formatter.hasNext());
    formatter.next();
    assertFalse(formatter.hasNext());
  }

  @Test
  public void testAggregate() {
    data.put(new Key("t", "~tab", "loc"), new Value("srv1".getBytes(UTF_8)));
    data.put(new Key("t;19700101", "~tab", "loc", 0), new Value("srv1".getBytes(UTF_8)));
    data.put(new Key("t;19700101", "~tab", "loc", 1), new Value("srv2".getBytes(UTF_8)));

    formatter.initialize(data.entrySet(), new FormatterConfig());

    String[] resultLines = formatter.next().split("\n");
    List<String> results = Arrays.asList(resultLines).subList(2, 4);

    assertTrue(CollectionUtils.exists(results, new AggregateReportChecker("NULL", 1)));
    assertTrue(CollectionUtils.exists(results, new AggregateReportChecker("19700101", 2)));

    assertFalse(formatter.hasNext());
  }

  private static class AggregateReportChecker implements Predicate {
    private String day;
    private int count;

    AggregateReportChecker(String day, int count) {
      this.day = day;
      this.count = count;
    }

    @Override
    public boolean evaluate(Object arg) {
      String resLine = (String) arg;
      return resLine.startsWith(this.day) && resLine.endsWith("" + this.count);
    }

  }
}
