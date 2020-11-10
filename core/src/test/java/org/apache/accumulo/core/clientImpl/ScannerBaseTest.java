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
package org.apache.accumulo.core.clientImpl;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class ScannerBaseTest {

  private ScannerBase s;
  private Map<Key,Value> scannerMap;
  private Iterator<Map.Entry<Key,Value>> it;
  private Key key;
  private Value val;
  private BiConsumer<Key,Value> keyValueConsumer;
  private forEachTester fet;

  private static class forEachTester {

    private Map<Key,Value> map;

    forEachTester(Map<Key,Value> map) {
      this.map = map;
    }

    public void forEach(BiConsumer<? super Key,? super Value> keyValueConsumer) {
      for (Map.Entry<Key,Value> entry : this.map.entrySet()) {
        keyValueConsumer.accept(entry.getKey(), entry.getValue());
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    s = createMock(ScannerBase.class);
  }

  @Test
  public void testScannerBaseForEach() throws Exception {
    key = new Key(new Text("a"), new Text("cf1"), new Text("cq1"));
    val = new Value(new Text("v1"));
    scannerMap = new HashMap<>();

    scannerMap.put(key, val);

    fet = new forEachTester(scannerMap);

    it = scannerMap.entrySet().iterator();

    expect(s.iterator()).andReturn(it).times(3);
    replay(s);

    Map<Key,Value> map = new HashMap<>();

    class MyBiConsumer implements BiConsumer<Key,Value> {
      @Override
      public void accept(Key key, Value value) {
        map.put(key, value);
      }
    }

    keyValueConsumer = new MyBiConsumer();

    fet.forEach(keyValueConsumer);

    // Test the Scanner values put into the map via keyValueConsumer

    for (Map.Entry<Key,Value> entry : map.entrySet()) {
      Map.Entry<Key,Value> expectedEntry = s.iterator().next();
      Key expectedKey = expectedEntry.getKey();
      Value expectedValue = expectedEntry.getValue();

      String expectedCf = expectedKey.getColumnFamily().toString();
      String actualCf = entry.getKey().getColumnFamily().toString();

      String expectedCq = expectedKey.getColumnQualifier().toString();
      String actualCq = entry.getKey().getColumnQualifier().toString();

      String expectedVal = expectedValue.toString();
      String actualVal = entry.getValue().toString();

      assertEquals(expectedCf, actualCf);
      assertEquals(expectedCq, actualCq);
      assertEquals(expectedVal, actualVal);
    }
  }
}
