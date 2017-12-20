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
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class ConcurrencyIT extends AccumuloClusterHarness {

  static class ScanTask extends Thread {

    int count = 0;
    Scanner scanner = null;

    ScanTask(Connector conn, String tableName, long time) throws Exception {
      try {
        scanner = conn.createScanner(tableName, Authorizations.EMPTY);
        IteratorSetting slow = new IteratorSetting(30, "slow", SlowIterator.class);
        SlowIterator.setSleepTime(slow, time);
        scanner.addScanIterator(slow);
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
    }

    @Override
    public void run() {
      count = Iterators.size(scanner.iterator());
    }

  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "1");
    cfg.setSiteConfig(siteConfig);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  // @formatter:off
  // Below is a diagram of the operations in this test over time.
  //
  // Scan 0 |------------------------------|
  // Scan 1 |----------|
  // Minc 1  |-----|
  // Scan 2   |----------|
  // Scan 3               |---------------|
  // Minc 2                |-----|
  // Majc 1                       |-----|
  // @formatter:on
  @Test
  public void run() throws Exception {
    Connector c = getConnector();
    runTest(c, getUniqueNames(1)[0]);
  }

  static void runTest(Connector c, String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException,
      MutationsRejectedException, Exception, InterruptedException {
    c.tableOperations().create(tableName);
    IteratorSetting is = new IteratorSetting(10, SlowIterator.class);
    SlowIterator.setSleepTime(is, 50);
    c.tableOperations().attachIterator(tableName, is, EnumSet.of(IteratorScope.minc, IteratorScope.majc));
    c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "1.0");

    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    for (int i = 0; i < 50; i++) {
      Mutation m = new Mutation(new Text(String.format("%06d", i)));
      m.put(new Text("cf1"), new Text("cq1"), new Value("foo".getBytes(UTF_8)));
      bw.addMutation(m);
    }
    bw.flush();

    ScanTask st0 = new ScanTask(c, tableName, 300);
    st0.start();

    ScanTask st1 = new ScanTask(c, tableName, 100);
    st1.start();

    sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
    c.tableOperations().flush(tableName, null, null, true);

    for (int i = 0; i < 50; i++) {
      Mutation m = new Mutation(new Text(String.format("%06d", i)));
      m.put(new Text("cf1"), new Text("cq1"), new Value("foo".getBytes(UTF_8)));
      bw.addMutation(m);
    }

    bw.flush();

    ScanTask st2 = new ScanTask(c, tableName, 100);
    st2.start();

    st1.join();
    st2.join();
    if (st1.count != 50)
      throw new Exception("Thread 1 did not see 50, saw " + st1.count);

    if (st2.count != 50)
      throw new Exception("Thread 2 did not see 50, saw " + st2.count);

    ScanTask st3 = new ScanTask(c, tableName, 150);
    st3.start();

    sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
    c.tableOperations().flush(tableName, null, null, false);

    st3.join();
    if (st3.count != 50)
      throw new Exception("Thread 3 did not see 50, saw " + st3.count);

    st0.join();
    if (st0.count != 50)
      throw new Exception("Thread 0 did not see 50, saw " + st0.count);

    bw.close();
  }
}
