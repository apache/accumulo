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
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ScannerBaseTest {

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

  @Test
  public void testScannerBaseForEach() throws Exception {

    ScannerBase s;
    Map<Key,Value> scannerMap;
    Iterator<Map.Entry<Key,Value>> it;
    Key key;
    Value val;
    BiConsumer<Key,Value> keyValueConsumer;
    forEachTester fet;

    s = createMock(ScannerBase.class);
    key = new Key(new Text("a"), new Text("cf1"), new Text("cq1"));
    val = new Value(new Text("v1"));
    scannerMap = new HashMap<>();

    scannerMap.put(key, val);

    fet = new forEachTester(scannerMap);

    it = scannerMap.entrySet().iterator();

    expect(s.iterator()).andReturn(it).times(1);
    replay(s);

    Map<Key,Value> map = new HashMap<>();
    fet.forEach(map::put);
    // Test the Scanner values put into the map via keyValueConsumer
    for (Map.Entry<Key,Value> entry : map.entrySet()) {
      Map.Entry<Key,Value> expectedEntry = s.iterator().next();
      assertEquals(expectedEntry.getKey(), entry.getKey());
      assertEquals(expectedEntry.getValue(), entry.getValue());
    }
    verify(s);
  }
}
