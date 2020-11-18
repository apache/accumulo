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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ScannerBaseTest {

  @Test
  public void testScannerBaseForEach() throws Exception {

    // This subclass of ScannerBase contains a List that ScannerBase.forEach() can
    // iterate over for testing purposes.
    class MockScanner implements ScannerBase {

      Map<Key,Value> map;

      MockScanner(Map<Key,Value> map) {
        this.map = map;
      }

      @Override
      public void addScanIterator(IteratorSetting cfg) {}

      @Override
      public void removeScanIterator(String iteratorName) {}

      @Override
      public void updateScanIteratorOption(String iteratorName, String key, String value) {}

      @Override
      public void fetchColumnFamily(Text col) {}

      @Override
      public void fetchColumn(Text colFam, Text colQual) {}

      @Override
      public void fetchColumn(IteratorSetting.Column column) {}

      @Override
      public void clearColumns() {}

      @Override
      public void clearScanIterators() {}

      @Override
      public Iterator<Map.Entry<Key,Value>> iterator() {
        return this.map.entrySet().iterator();
      }

      @Override
      public void setTimeout(long timeOut, TimeUnit timeUnit) {}

      @Override
      public long getTimeout(TimeUnit timeUnit) {
        return 0;
      }

      @Override
      public void close() {}

      @Override
      public Authorizations getAuthorizations() {
        return null;
      }

      @Override
      public void setSamplerConfiguration(SamplerConfiguration samplerConfig) {}

      @Override
      public SamplerConfiguration getSamplerConfiguration() {
        return null;
      }

      @Override
      public void clearSamplerConfiguration() {}

      @Override
      public void setBatchTimeout(long timeOut, TimeUnit timeUnit) {}

      @Override
      public long getBatchTimeout(TimeUnit timeUnit) {
        return 0;
      }

      @Override
      public void setClassLoaderContext(String classLoaderContext) {}

      @Override
      public void clearClassLoaderContext() {}

      @Override
      public String getClassLoaderContext() {
        return null;
      }
    }

    Map<Key,Value> map = new HashMap<>();
    MockScanner ms = new MockScanner(Map
        .of(new Key(new Text("a"), new Text("cf1"), new Text("cq1")), new Value(new Text("v1"))));
    Map.Entry entry = ms.iterator().next();
    // Test forEach from ScannerBase
    ms.forEach((k, v) -> map.put(k, v));

    for (Map.Entry<Key,Value> e : map.entrySet()) {
      assertEquals(entry.getKey(), e.getKey());
      assertEquals(entry.getValue(), e.getValue());
    }
  }
}
