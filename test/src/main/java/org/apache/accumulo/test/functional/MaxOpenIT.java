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
package org.apache.accumulo.test.functional;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.IngestParams;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A functional test that exercises hitting the max open file limit on a tablet server. This test
 * assumes there are one or two tablet servers.
 */
@SuppressWarnings("removal")
public class MaxOpenIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> conf = cfg.getSiteConfig();
    conf.put(Property.TSERV_SCAN_MAX_OPENFILES.getKey(), "4");
    conf.put(Property.TSERV_MAJC_MAXCONCURRENT.getKey(), "1");
    conf.put(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey(), "2");
    cfg.setSiteConfig(conf);
  }

  private String scanMaxOpenFiles, majcConcurrent, majcThreadMaxOpen;

  @BeforeEach
  public void alterConfig() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      InstanceOperations iops = client.instanceOperations();
      Map<String,String> sysConfig = iops.getSystemConfiguration();
      scanMaxOpenFiles = sysConfig.get(Property.TSERV_SCAN_MAX_OPENFILES.getKey());
      majcConcurrent = sysConfig.get(Property.TSERV_MAJC_MAXCONCURRENT.getKey());
      majcThreadMaxOpen = sysConfig.get(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey());
    }
  }

  @AfterEach
  public void restoreConfig() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      InstanceOperations iops = client.instanceOperations();
      if (scanMaxOpenFiles != null) {
        iops.setProperty(Property.TSERV_SCAN_MAX_OPENFILES.getKey(), scanMaxOpenFiles);
      }
      if (majcConcurrent != null) {
        iops.setProperty(Property.TSERV_MAJC_MAXCONCURRENT.getKey(), majcConcurrent);
      }
      if (majcThreadMaxOpen != null) {
        iops.setProperty(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey(), majcThreadMaxOpen);
      }
    }
  }

  private static final int NUM_TABLETS = 16;
  private static final int NUM_TO_INGEST = 10000;

  @Test
  public void run() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      HashMap<String,String> props = new HashMap<>();
      props.put(Property.TABLE_MAJC_RATIO.getKey(), "10");
      NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props)
          .withSplits(TestIngest.getSplitPoints(0, NUM_TO_INGEST, NUM_TABLETS));
      c.tableOperations().create(tableName, ntc);

      // the following loop should create three tablets in each map file
      for (int i = 0; i < 3; i++) {
        IngestParams params = new IngestParams(getClientProps(), tableName, NUM_TO_INGEST);
        params.timestamp = i;
        params.dataSize = 50;
        params.rows = NUM_TO_INGEST;
        params.cols = 1;
        params.random = i;
        TestIngest.ingest(c, params);

        c.tableOperations().flush(tableName, null, null, true);
        FunctionalTestUtils.checkRFiles(c, tableName, NUM_TABLETS, NUM_TABLETS, i + 1, i + 1);
      }

      List<Range> ranges = new ArrayList<>(NUM_TO_INGEST);

      for (int i = 0; i < NUM_TO_INGEST; i++) {
        ranges.add(new Range(TestIngest.generateRow(i, 0)));
      }

      long time1 = batchScan(c, tableName, ranges, 1);
      // run it again, now that stuff is cached on the client and sever
      time1 = batchScan(c, tableName, ranges, 1);
      long time2 = batchScan(c, tableName, ranges, NUM_TABLETS);

      System.out.printf("Single thread scan time   %6.2f %n", time1 / 1000.0);
      System.out.printf("Multiple thread scan time %6.2f %n", time2 / 1000.0);
    }
  }

  private long batchScan(AccumuloClient c, String tableName, List<Range> ranges, int threads)
      throws Exception {
    try (BatchScanner bs = c.createBatchScanner(tableName, TestIngest.AUTHS, threads)) {

      bs.setRanges(ranges);

      int count = 0;

      long t1 = System.currentTimeMillis();

      byte[] rval = new byte[50];

      for (Entry<Key,Value> entry : bs) {
        count++;
        int row = VerifyIngest.getRow(entry.getKey());
        int col = VerifyIngest.getCol(entry.getKey());

        if (row < 0 || row >= NUM_TO_INGEST) {
          throw new Exception("unexpected row " + row);
        }

        rval = TestIngest.genRandomValue(rval, 2, row, col);

        if (entry.getValue().compareTo(rval) != 0) {
          throw new Exception("unexpected value row=" + row + " col=" + col);
        }
      }

      long t2 = System.currentTimeMillis();

      if (count != NUM_TO_INGEST) {
        throw new Exception("Batch Scan did not return expected number of values " + count);
      }

      return t2 - t1;
    }
  }

}
