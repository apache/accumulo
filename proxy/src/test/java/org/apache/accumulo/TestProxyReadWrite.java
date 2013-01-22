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
package org.apache.accumulo;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.proxy.Proxy;
import org.apache.accumulo.proxy.TestProxyClient;
import org.apache.accumulo.proxy.Util;
import org.apache.accumulo.proxy.thrift.PColumnUpdate;
import org.apache.accumulo.proxy.thrift.PIteratorSetting;
import org.apache.accumulo.proxy.thrift.PKey;
import org.apache.accumulo.proxy.thrift.PKeyValue;
import org.apache.accumulo.proxy.thrift.PRange;
import org.apache.accumulo.proxy.thrift.PScanResult;
import org.apache.accumulo.proxy.thrift.UserPass;
import org.apache.thrift.server.TServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestProxyReadWrite {
  protected static TServer proxy;
  protected static Thread thread;
  protected static TestProxyClient tpc;
  protected static UserPass userpass;
  protected static final int port = 10194;
  protected static final String testtable = "testtable";
  
  @BeforeClass
  public static void setup() throws Exception {
    Properties prop = new Properties();
    prop.setProperty("org.apache.accumulo.proxy.ProxyServer.useMockInstance", "true");
    
    proxy = Proxy.createProxyServer(Class.forName("org.apache.accumulo.proxy.thrift.AccumuloProxy"),
        Class.forName("org.apache.accumulo.proxy.ProxyServer"), port, prop);
    thread = new Thread() {
      @Override
      public void run() {
        proxy.serve();
      }
    };
    thread.start();
    tpc = new TestProxyClient("localhost", port);
    userpass = new UserPass("root", ByteBuffer.wrap("".getBytes()));
  }
  
  @AfterClass
  public static void tearDown() throws InterruptedException {
    proxy.stop();
    thread.join();
  }
  
  @Before
  public void makeTestTable() throws Exception {
    tpc.proxy().tableOperations_create(userpass, testtable);
  }
  
  @After
  public void deleteTestTable() throws Exception {
    tpc.proxy().tableOperations_delete(userpass, testtable);
  }
  
  private static void addMutation(Map<ByteBuffer,List<PColumnUpdate>> mutations, String row, String cf, String cq, String value) {
    PColumnUpdate update = new PColumnUpdate(ByteBuffer.wrap(cf.getBytes()), ByteBuffer.wrap(cq.getBytes()));
    update.setValue(value.getBytes());
    mutations.put(ByteBuffer.wrap(row.getBytes()), Collections.singletonList(update));
  }
  
  private static void addMutation(Map<ByteBuffer,List<PColumnUpdate>> mutations, String row, String cf, String cq, String vis, String value) {
    PColumnUpdate update = new PColumnUpdate(ByteBuffer.wrap(cf.getBytes()), ByteBuffer.wrap(cq.getBytes()));
    update.setValue(value.getBytes());
    update.setColVisibility(vis.getBytes());
    mutations.put(ByteBuffer.wrap(row.getBytes()), Collections.singletonList(update));
  }
  
  /**
   * Insert 100000 cells which have as the row [0..99999] (padded with zeros). Set a range so only the entries between -Inf...5 come back (there should be
   * 50,000)
   * 
   * @throws Exception
   */
  @Test
  public void readWriteBatchOneShotWithRange() throws Exception {
    int maxInserts = 100000;
    Map<ByteBuffer,List<PColumnUpdate>> mutations = new HashMap<ByteBuffer,List<PColumnUpdate>>();
    String format = "%1$05d";
    for (int i = 0; i < maxInserts; i++) {
      addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, Util.randString(10));
      
      if (i % 1000 == 0 || i == maxInserts - 1) {
        tpc.proxy().updateAndFlush(userpass, testtable, mutations);
        mutations.clear();
      }
    }
    
    PKey stop = new PKey();
    stop.setRow("5".getBytes());
    List<PRange> pranges = new ArrayList<PRange>();
    pranges.add(new PRange(null, false, stop, false));
    String cookie = tpc.proxy().createBatchScanner(userpass, testtable, null, null, pranges);
    
    int i = 0;
    boolean hasNext = true;
    
    int k = 1000;
    while (hasNext) {
      PScanResult kvList = tpc.proxy().scanner_next_k(cookie, k);
      i += kvList.getResultsSize();
      hasNext = kvList.isMore();
    }
    assertEquals(i, 50000);
  }
  
  /**
   * Insert 100000 cells which have as the row [0..99999] (padded with zeros). Filter the results so only the even numbers come back.
   * 
   * @throws Exception
   */
  @Test
  public void readWriteBatchOneShotWithFilterIterator() throws Exception {
    int maxInserts = 10000;
    Map<ByteBuffer,List<PColumnUpdate>> mutations = new HashMap<ByteBuffer,List<PColumnUpdate>>();
    String format = "%1$05d";
    for (int i = 0; i < maxInserts; i++) {
      addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, Util.randString(10));
      
      if (i % 1000 == 0 || i == maxInserts - 1) {
        tpc.proxy().updateAndFlush(userpass, testtable, mutations);
        mutations.clear();
      }
      
    }
    
    String regex = ".*[02468]";
    
    IteratorSetting is = new IteratorSetting(50, regex, RegExFilter.class);
    RegExFilter.setRegexs(is, regex, null, null, null, false);
    
    PIteratorSetting pis = Util.iteratorSetting2ProxyIteratorSetting(is);
    String cookie = tpc.proxy().createBatchScanner(userpass, testtable, null, pis, null);
    
    int i = 0;
    boolean hasNext = true;
    
    int k = 1000;
    while (hasNext) {
      PScanResult kvList = tpc.proxy().scanner_next_k(cookie, k);
      for (PKeyValue kv : kvList.getResults()) {
        assertEquals(Integer.parseInt(new String(kv.getKey().getRow())), i);
        
        i += 2;
      }
      hasNext = kvList.isMore();
    }
  }
  
  @Test
  public void readWriteOneShotWithRange() throws Exception {
    int maxInserts = 100000;
    Map<ByteBuffer,List<PColumnUpdate>> mutations = new HashMap<ByteBuffer,List<PColumnUpdate>>();
    String format = "%1$05d";
    for (int i = 0; i < maxInserts; i++) {
      addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, Util.randString(10));
      
      if (i % 1000 == 0 || i == maxInserts - 1) {
        tpc.proxy().updateAndFlush(userpass, testtable, mutations);
        mutations.clear();
      }
    }
    
    PKey stop = new PKey();
    stop.setRow("5".getBytes());
    String cookie = tpc.proxy().createScanner(userpass, testtable, null, null, new PRange(null, false, stop, false));
    
    int i = 0;
    boolean hasNext = true;
    
    int k = 1000;
    while (hasNext) {
      PScanResult kvList = tpc.proxy().scanner_next_k(cookie, k);
      i += kvList.getResultsSize();
      hasNext = kvList.isMore();
    }
    assertEquals(i, 50000);
  }
  
  /**
   * Insert 100000 cells which have as the row [0..99999] (padded with zeros). Filter the results so only the even numbers come back.
   * 
   * @throws Exception
   */
  @Test
  public void readWriteOneShotWithFilterIterator() throws Exception {
    int maxInserts = 10000;
    Map<ByteBuffer,List<PColumnUpdate>> mutations = new HashMap<ByteBuffer,List<PColumnUpdate>>();
    String format = "%1$05d";
    for (int i = 0; i < maxInserts; i++) {
      addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, Util.randString(10));
      
      if (i % 1000 == 0 || i == maxInserts - 1) {
        
        tpc.proxy().updateAndFlush(userpass, testtable, mutations);
        mutations.clear();
        
      }
      
    }
    
    String regex = ".*[02468]";
    
    IteratorSetting is = new IteratorSetting(50, regex, RegExFilter.class);
    RegExFilter.setRegexs(is, regex, null, null, null, false);
    
    PIteratorSetting pis = Util.iteratorSetting2ProxyIteratorSetting(is);
    String cookie = tpc.proxy().createScanner(userpass, testtable, null, pis, null);
    
    int i = 0;
    boolean hasNext = true;
    
    int k = 1000;
    while (hasNext) {
      PScanResult kvList = tpc.proxy().scanner_next_k(cookie, k);
      for (PKeyValue kv : kvList.getResults()) {
        assertEquals(Integer.parseInt(new String(kv.getKey().getRow())), i);
        
        i += 2;
      }
      hasNext = kvList.isMore();
    }
  }
  
  // @Test
  // This test takes kind of a long time. Enable it if you think you may have memory issues.
  public void manyWritesAndReads() throws Exception {
    int maxInserts = 1000000;
    Map<ByteBuffer,List<PColumnUpdate>> mutations = new HashMap<ByteBuffer,List<PColumnUpdate>>();
    String format = "%1$06d";
    String writer = tpc.proxy().createWriter(userpass, testtable);
    for (int i = 0; i < maxInserts; i++) {
      addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, Util.randString(10));
      
      if (i % 1000 == 0 || i == maxInserts - 1) {
        
        tpc.proxy().writer_update(writer, mutations);
        mutations.clear();
        
      }
      
    }
    
    tpc.proxy().writer_flush(writer);
    tpc.proxy().writer_close(writer);
    
    String cookie = tpc.proxy().createBatchScanner(userpass, testtable, null, null, null);
    
    int i = 0;
    boolean hasNext = true;
    
    int k = 1000;
    while (hasNext) {
      PScanResult kvList = tpc.proxy().scanner_next_k(cookie, k);
      for (PKeyValue kv : kvList.getResults()) {
        assertEquals(Integer.parseInt(new String(kv.getKey().getRow())), i);
        i++;
      }
      hasNext = kvList.isMore();
      if (hasNext)
        assertEquals(k, kvList.getResults().size());
    }
    assertEquals(maxInserts, i);
  }
  
  @Test
  public void asynchReadWrite() throws Exception {
    int maxInserts = 10000;
    Map<ByteBuffer,List<PColumnUpdate>> mutations = new HashMap<ByteBuffer,List<PColumnUpdate>>();
    String format = "%1$05d";
    String writer = tpc.proxy().createWriter(userpass, testtable);
    for (int i = 0; i < maxInserts; i++) {
      addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, Util.randString(10));
      
      if (i % 1000 == 0 || i == maxInserts - 1) {
        tpc.proxy().writer_update(writer, mutations);
        mutations.clear();
      }
    }
    
    tpc.proxy().writer_flush(writer);
    tpc.proxy().writer_close(writer);
    
    String regex = ".*[02468]";
    
    IteratorSetting is = new IteratorSetting(50, regex, RegExFilter.class);
    RegExFilter.setRegexs(is, regex, null, null, null, false);
    
    PIteratorSetting pis = Util.iteratorSetting2ProxyIteratorSetting(is);
    String cookie = tpc.proxy().createBatchScanner(userpass, testtable, null, pis, null);
    
    int i = 0;
    boolean hasNext = true;
    
    int k = 1000;
    int numRead = 0;
    while (hasNext) {
      PScanResult kvList = tpc.proxy().scanner_next_k(cookie, k);
      for (PKeyValue kv : kvList.getResults()) {
        assertEquals(i, Integer.parseInt(new String(kv.getKey().getRow())));
        numRead++;
        i += 2;
      }
      hasNext = kvList.isMore();
    }
    assertEquals(maxInserts / 2, numRead);
  }
  
  @Test
  public void testVisibility() throws Exception {
    
    Set<String> auths = new HashSet<String>();
    auths.add("even");
    tpc.proxy().securityOperations_changeUserAuthorizations(userpass, "root", auths);
    
    int maxInserts = 10000;
    Map<ByteBuffer,List<PColumnUpdate>> mutations = new HashMap<ByteBuffer,List<PColumnUpdate>>();
    String format = "%1$05d";
    String writer = tpc.proxy().createWriter(userpass, testtable);
    for (int i = 0; i < maxInserts; i++) {
      if (i % 2 == 0)
        addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, "even", Util.randString(10));
      else
        addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, "odd", Util.randString(10));
      
      if (i % 1000 == 0 || i == maxInserts - 1) {
        tpc.proxy().writer_update(writer, mutations);
        mutations.clear();
      }
    }
    
    tpc.proxy().writer_flush(writer);
    tpc.proxy().writer_close(writer);
    String cookie = tpc.proxy().createBatchScanner(userpass, testtable, auths, null, null);
    
    int i = 0;
    boolean hasNext = true;
    
    int k = 1000;
    int numRead = 0;
    while (hasNext) {
      PScanResult kvList = tpc.proxy().scanner_next_k(cookie, k);
      for (PKeyValue kv : kvList.getResults()) {
        assertEquals(Integer.parseInt(new String(kv.getKey().getRow())), i);
        i += 2;
        numRead++;
      }
      hasNext = kvList.isMore();
      
    }
    assertEquals(maxInserts / 2, numRead);
  }
  
}
