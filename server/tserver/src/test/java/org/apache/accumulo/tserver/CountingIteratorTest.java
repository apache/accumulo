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
package org.apache.accumulo.tserver;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.accumulo.server.compaction.CountingIterator;
import org.junit.jupiter.api.Test;

public class CountingIteratorTest {

  @Test
  public void testDeepCopyCount() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<>();

    tm.put(new Key("r1", "cf1", "cq1"), new Value("data1"));
    tm.put(new Key("r2", "cf1", "cq1"), new Value("data2"));

    SortedMapIterator smi = new SortedMapIterator(tm);

    CountingIterator ci = new CountingIterator(smi, new AtomicLong(0));
    CountingIterator dc1 = ci.deepCopy(null);
    CountingIterator dc2 = ci.deepCopy(null);

    readAll(ci);
    readAll(dc1);
    readAll(dc2);

    assertEquals(6, ci.getCount());
  }

  private void readAll(CountingIterator ci) throws IOException {
    ci.seek(new Range(), new HashSet<>(), false);
    while (ci.hasTop()) {
      ci.next();
    }
  }
}
