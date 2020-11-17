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
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.awt.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.shaded.org.mockito.Mockito;
import org.easymock.EasyMock;
import org.junit.Test;

public class ScannerBaseTest {

  @Test
  public void testScannerBaseForEach() throws Exception {

    //This subclass of ScannerBase contains a List that ScannerBase.forEach() can
    //iterate over for testing purposes.
    class MockScanner extends List implements ScannerBase {

      private ArrayList<Map.Entry<Key, Value>> list = new ArrayList<>();

      public void add (Map.Entry<Key, Value> entry) {
        this.list.add(entry);
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
      public Iterator<Map.Entry<Key, Value>> iterator() {
        return list.iterator();
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

    MockScanner mockScanner = new MockScanner();
    Map<Key,Value> scannerMap = new HashMap<>();
    Map<Key,Value> map = new HashMap<>();

    scannerMap.put(new Key(new Text("a"), new Text("cf1"), new Text("cq1")),
        new Value(new Text("v1")));

    Iterator<Map.Entry<Key,Value>> it = scannerMap.entrySet().iterator();
    Map.Entry entry = it.next();
    mockScanner.add(entry);
    //Test forEach from ScannerBase
    mockScanner.forEach((k,v) -> map.put(k,v));

    for (Map.Entry<Key,Value> e : map.entrySet()) {
      assertEquals(entry.getKey(), e.getKey());
      assertEquals(entry.getValue(), e.getValue());
    }
  }
}
