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
package org.apache.accumulo.proxy;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.proxy.thrift.BatchScanOptions;
import org.apache.accumulo.proxy.thrift.ColumnUpdate;
import org.apache.accumulo.proxy.thrift.IteratorSetting;
import org.apache.accumulo.proxy.thrift.Key;
import org.apache.accumulo.proxy.thrift.KeyValue;
import org.apache.accumulo.proxy.thrift.Range;
import org.apache.accumulo.proxy.thrift.ScanColumn;
import org.apache.accumulo.proxy.thrift.ScanOptions;
import org.apache.accumulo.proxy.thrift.ScanResult;
import org.apache.accumulo.proxy.thrift.TimeType;
import org.apache.thrift.protocol.TCompactProtocol;
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
  protected static ByteBuffer userpass;
  protected static final int port = 10194;
  protected static final String testtable = "testtable";

  @SuppressWarnings("serial")
  @BeforeClass
  public static void setup() throws Exception {
    Properties prop = new Properties();
    prop.setProperty("useMockInstance", "true");
    prop.put("tokenClass", PasswordToken.class.getName());

    proxy = Proxy.createProxyServer(Class.forName("org.apache.accumulo.proxy.thrift.AccumuloProxy"), Class.forName("org.apache.accumulo.proxy.ProxyServer"),
        port, TCompactProtocol.Factory.class, prop);
    thread = new Thread() {
      @Override
      public void run() {
        proxy.serve();
      }
    };
    thread.start();
    tpc = new TestProxyClient("localhost", port);
    userpass = tpc.proxy().login("root", new TreeMap<String,String>() {
      {
        put("password", "");
      }
    });
  }

  @AfterClass
  public static void tearDown() throws InterruptedException {
    proxy.stop();
    thread.join();
  }

  @Before
  public void makeTestTable() throws Exception {
    tpc.proxy().createTable(userpass, testtable, true, TimeType.MILLIS);
  }

  @After
  public void deleteTestTable() throws Exception {
    tpc.proxy().deleteTable(userpass, testtable);
  }

  private static void addMutation(Map<ByteBuffer,List<ColumnUpdate>> mutations, String row, String cf, String cq, String value) {
    ColumnUpdate update = new ColumnUpdate(ByteBuffer.wrap(cf.getBytes()), ByteBuffer.wrap(cq.getBytes()));
    update.setValue(value.getBytes());
    mutations.put(ByteBuffer.wrap(row.getBytes()), Collections.singletonList(update));
  }

  private static void addMutation(Map<ByteBuffer,List<ColumnUpdate>> mutations, String row, String cf, String cq, String vis, String value) {
    ColumnUpdate update = new ColumnUpdate(ByteBuffer.wrap(cf.getBytes()), ByteBuffer.wrap(cq.getBytes()));
    update.setValue(value.getBytes());
    update.setColVisibility(vis.getBytes());
    mutations.put(ByteBuffer.wrap(row.getBytes()), Collections.singletonList(update));
  }

  /**
   * Insert 100000 cells which have as the row [0..99999] (padded with zeros). Set a range so only the entries between -Inf...5 come back (there should be
   * 50,000)
   */
  @Test
  public void readWriteBatchOneShotWithRange() throws Exception {
    int maxInserts = 100000;
    Map<ByteBuffer,List<ColumnUpdate>> mutations = new HashMap<ByteBuffer,List<ColumnUpdate>>();
    String format = "%1$05d";
    for (int i = 0; i < maxInserts; i++) {
      addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, Util.randString(10));

      if (i % 1000 == 0 || i == maxInserts - 1) {
        tpc.proxy().updateAndFlush(userpass, testtable, mutations);
        mutations.clear();
      }
    }

    Key stop = new Key();
    stop.setRow("5".getBytes());
    BatchScanOptions options = new BatchScanOptions();
    options.ranges = Collections.singletonList(new Range(null, false, stop, false));
    String cookie = tpc.proxy().createBatchScanner(userpass, testtable, options);

    int i = 0;
    boolean hasNext = true;

    int k = 1000;
    while (hasNext) {
      ScanResult kvList = tpc.proxy().nextK(cookie, k);
      i += kvList.getResultsSize();
      hasNext = kvList.isMore();
    }
    assertEquals(i, 50000);
  }

  /**
   * Insert 100000 cells which have as the row [0..99999] (padded with zeros). Set a columnFamily so only the entries with specified column family come back
   * (there should be 50,000)
   */
  @Test
  public void readWriteBatchOneShotWithColumnFamilyOnly() throws Exception {
    int maxInserts = 100000;
    Map<ByteBuffer,List<ColumnUpdate>> mutations = new HashMap<ByteBuffer,List<ColumnUpdate>>();
    String format = "%1$05d";
    for (int i = 0; i < maxInserts; i++) {

      addMutation(mutations, String.format(format, i), "cf" + (i % 2), "cq" + (i % 2), Util.randString(10));

      if (i % 1000 == 0 || i == maxInserts - 1) {
        tpc.proxy().updateAndFlush(userpass, testtable, mutations);
        mutations.clear();
      }
    }

    BatchScanOptions options = new BatchScanOptions();

    ScanColumn sc = new ScanColumn();
    sc.colFamily = ByteBuffer.wrap("cf0".getBytes());

    options.columns = Collections.singletonList(sc);
    String cookie = tpc.proxy().createBatchScanner(userpass, testtable, options);

    int i = 0;
    boolean hasNext = true;

    int k = 1000;
    while (hasNext) {
      ScanResult kvList = tpc.proxy().nextK(cookie, k);
      i += kvList.getResultsSize();
      hasNext = kvList.isMore();
    }
    assertEquals(i, 50000);
  }

  /**
   * Insert 100000 cells which have as the row [0..99999] (padded with zeros). Set a columnFamily + columnQualififer so only the entries with specified column
   * come back (there should be 50,000)
   */
  @Test
  public void readWriteBatchOneShotWithFullColumn() throws Exception {
    int maxInserts = 100000;
    Map<ByteBuffer,List<ColumnUpdate>> mutations = new HashMap<ByteBuffer,List<ColumnUpdate>>();
    String format = "%1$05d";
    for (int i = 0; i < maxInserts; i++) {

      addMutation(mutations, String.format(format, i), "cf" + (i % 2), "cq" + (i % 2), Util.randString(10));

      if (i % 1000 == 0 || i == maxInserts - 1) {
        tpc.proxy().updateAndFlush(userpass, testtable, mutations);
        mutations.clear();
      }
    }

    BatchScanOptions options = new BatchScanOptions();

    ScanColumn sc = new ScanColumn();
    sc.colFamily = ByteBuffer.wrap("cf0".getBytes());
    sc.colQualifier = ByteBuffer.wrap("cq0".getBytes());

    options.columns = Collections.singletonList(sc);
    String cookie = tpc.proxy().createBatchScanner(userpass, testtable, options);

    int i = 0;
    boolean hasNext = true;

    int k = 1000;
    while (hasNext) {
      ScanResult kvList = tpc.proxy().nextK(cookie, k);
      i += kvList.getResultsSize();
      hasNext = kvList.isMore();
    }
    assertEquals(i, 50000);
  }

  /**
   * Insert 100000 cells which have as the row [0..99999] (padded with zeros). Filter the results so only the even numbers come back.
   */
  @Test
  public void readWriteBatchOneShotWithFilterIterator() throws Exception {
    int maxInserts = 10000;
    Map<ByteBuffer,List<ColumnUpdate>> mutations = new HashMap<ByteBuffer,List<ColumnUpdate>>();
    String format = "%1$05d";
    for (int i = 0; i < maxInserts; i++) {
      addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, Util.randString(10));

      if (i % 1000 == 0 || i == maxInserts - 1) {
        tpc.proxy().updateAndFlush(userpass, testtable, mutations);
        mutations.clear();
      }

    }

    String regex = ".*[02468]";

    org.apache.accumulo.core.client.IteratorSetting is = new org.apache.accumulo.core.client.IteratorSetting(50, regex, RegExFilter.class);
    RegExFilter.setRegexs(is, regex, null, null, null, false);

    IteratorSetting pis = Util.iteratorSetting2ProxyIteratorSetting(is);
    ScanOptions opts = new ScanOptions();
    opts.iterators = Collections.singletonList(pis);
    String cookie = tpc.proxy().createScanner(userpass, testtable, opts);

    int i = 0;
    boolean hasNext = true;

    int k = 1000;
    while (hasNext) {
      ScanResult kvList = tpc.proxy().nextK(cookie, k);
      for (KeyValue kv : kvList.getResults()) {
        assertEquals(Integer.parseInt(new String(kv.getKey().getRow())), i);

        i += 2;
      }
      hasNext = kvList.isMore();
    }
  }

  @Test
  public void readWriteOneShotWithRange() throws Exception {
    int maxInserts = 100000;
    Map<ByteBuffer,List<ColumnUpdate>> mutations = new HashMap<ByteBuffer,List<ColumnUpdate>>();
    String format = "%1$05d";
    for (int i = 0; i < maxInserts; i++) {
      addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, Util.randString(10));

      if (i % 1000 == 0 || i == maxInserts - 1) {
        tpc.proxy().updateAndFlush(userpass, testtable, mutations);
        mutations.clear();
      }
    }

    Key stop = new Key();
    stop.setRow("5".getBytes());
    ScanOptions opts = new ScanOptions();
    opts.range = new Range(null, false, stop, false);
    String cookie = tpc.proxy().createScanner(userpass, testtable, opts);

    int i = 0;
    boolean hasNext = true;

    int k = 1000;
    while (hasNext) {
      ScanResult kvList = tpc.proxy().nextK(cookie, k);
      i += kvList.getResultsSize();
      hasNext = kvList.isMore();
    }
    assertEquals(i, 50000);
  }

  /**
   * Insert 100000 cells which have as the row [0..99999] (padded with zeros). Filter the results so only the even numbers come back.
   */
  @Test
  public void readWriteOneShotWithFilterIterator() throws Exception {
    int maxInserts = 10000;
    Map<ByteBuffer,List<ColumnUpdate>> mutations = new HashMap<ByteBuffer,List<ColumnUpdate>>();
    String format = "%1$05d";
    for (int i = 0; i < maxInserts; i++) {
      addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, Util.randString(10));

      if (i % 1000 == 0 || i == maxInserts - 1) {

        tpc.proxy().updateAndFlush(userpass, testtable, mutations);
        mutations.clear();

      }

    }

    String regex = ".*[02468]";

    org.apache.accumulo.core.client.IteratorSetting is = new org.apache.accumulo.core.client.IteratorSetting(50, regex, RegExFilter.class);
    RegExFilter.setRegexs(is, regex, null, null, null, false);

    IteratorSetting pis = Util.iteratorSetting2ProxyIteratorSetting(is);
    ScanOptions opts = new ScanOptions();
    opts.iterators = Collections.singletonList(pis);
    String cookie = tpc.proxy().createScanner(userpass, testtable, opts);

    int i = 0;
    boolean hasNext = true;

    int k = 1000;
    while (hasNext) {
      ScanResult kvList = tpc.proxy().nextK(cookie, k);
      for (KeyValue kv : kvList.getResults()) {
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
    Map<ByteBuffer,List<ColumnUpdate>> mutations = new HashMap<ByteBuffer,List<ColumnUpdate>>();
    String format = "%1$06d";
    String writer = tpc.proxy().createWriter(userpass, testtable, null);
    for (int i = 0; i < maxInserts; i++) {
      addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, Util.randString(10));

      if (i % 1000 == 0 || i == maxInserts - 1) {

        tpc.proxy().update(writer, mutations);
        mutations.clear();

      }

    }

    tpc.proxy().flush(writer);
    tpc.proxy().closeWriter(writer);

    String cookie = tpc.proxy().createScanner(userpass, testtable, null);

    int i = 0;
    boolean hasNext = true;

    int k = 1000;
    while (hasNext) {
      ScanResult kvList = tpc.proxy().nextK(cookie, k);
      for (KeyValue kv : kvList.getResults()) {
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
    Map<ByteBuffer,List<ColumnUpdate>> mutations = new HashMap<ByteBuffer,List<ColumnUpdate>>();
    String format = "%1$05d";
    String writer = tpc.proxy().createWriter(userpass, testtable, null);
    for (int i = 0; i < maxInserts; i++) {
      addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, Util.randString(10));

      if (i % 1000 == 0 || i == maxInserts - 1) {
        tpc.proxy().update(writer, mutations);
        mutations.clear();
      }
    }

    tpc.proxy().flush(writer);
    tpc.proxy().closeWriter(writer);

    String regex = ".*[02468]";

    org.apache.accumulo.core.client.IteratorSetting is = new org.apache.accumulo.core.client.IteratorSetting(50, regex, RegExFilter.class);
    RegExFilter.setRegexs(is, regex, null, null, null, false);

    IteratorSetting pis = Util.iteratorSetting2ProxyIteratorSetting(is);
    ScanOptions opts = new ScanOptions();
    opts.iterators = Collections.singletonList(pis);
    String cookie = tpc.proxy().createScanner(userpass, testtable, opts);

    int i = 0;
    boolean hasNext = true;

    int k = 1000;
    int numRead = 0;
    while (hasNext) {
      ScanResult kvList = tpc.proxy().nextK(cookie, k);
      for (KeyValue kv : kvList.getResults()) {
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

    Set<ByteBuffer> auths = new HashSet<ByteBuffer>();
    auths.add(ByteBuffer.wrap("even".getBytes()));
    tpc.proxy().changeUserAuthorizations(userpass, "root", auths);

    int maxInserts = 10000;
    Map<ByteBuffer,List<ColumnUpdate>> mutations = new HashMap<ByteBuffer,List<ColumnUpdate>>();
    String format = "%1$05d";
    String writer = tpc.proxy().createWriter(userpass, testtable, null);
    for (int i = 0; i < maxInserts; i++) {
      if (i % 2 == 0)
        addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, "even", Util.randString(10));
      else
        addMutation(mutations, String.format(format, i), "cf" + i, "cq" + i, "odd", Util.randString(10));

      if (i % 1000 == 0 || i == maxInserts - 1) {
        tpc.proxy().update(writer, mutations);
        mutations.clear();
      }
    }

    tpc.proxy().flush(writer);
    tpc.proxy().closeWriter(writer);
    ScanOptions opts = new ScanOptions();
    opts.authorizations = auths;
    String cookie = tpc.proxy().createScanner(userpass, testtable, opts);

    int i = 0;
    boolean hasNext = true;

    int k = 1000;
    int numRead = 0;
    while (hasNext) {
      ScanResult kvList = tpc.proxy().nextK(cookie, k);
      for (KeyValue kv : kvList.getResults()) {
        assertEquals(Integer.parseInt(new String(kv.getKey().getRow())), i);
        i += 2;
        numRead++;
      }
      hasNext = kvList.isMore();

    }
    assertEquals(maxInserts / 2, numRead);
  }

}
