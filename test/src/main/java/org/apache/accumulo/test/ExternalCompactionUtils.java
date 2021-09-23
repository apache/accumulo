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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.coordinator.ExternalCompactionMetrics;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.ExternalCompaction_1_IT.TestFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Maps;
import com.google.gson.Gson;

public class ExternalCompactionUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalCompactionUtils.class);

  public static final int MAX_DATA = 1000;

  public static String row(int r) {
    return String.format("r:%04d", r);
  }

  public static Stream<ExternalCompactionFinalState> getFinalStatesForTable(AccumuloCluster cluster,
      TableId tid) {
    return cluster.getServerContext().getAmple().getExternalCompactionFinalStates()
        .filter(state -> state.getExtent().tableId().equals(tid));
  }

  public static void compact(final AccumuloClient client, String table1, int modulus,
      String expectedQueue, boolean wait)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    IteratorSetting iterSetting = new IteratorSetting(100, TestFilter.class);
    // make sure iterator options make it to compactor process
    iterSetting.addOption("expectedQ", expectedQueue);
    iterSetting.addOption("modulus", modulus + "");
    CompactionConfig config =
        new CompactionConfig().setIterators(List.of(iterSetting)).setWait(wait);
    client.tableOperations().compact(table1, config);
  }

  public static void createTable(AccumuloClient client, String tableName, String service)
      throws Exception {
    Map<String,String> props =
        Map.of("table.compaction.dispatcher", SimpleCompactionDispatcher.class.getName(),
            "table.compaction.dispatcher.opts.service", service);
    NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props);

    client.tableOperations().create(tableName, ntc);

  }

  public static void createTable(AccumuloClient client, String tableName, String service,
      int numTablets) throws Exception {
    SortedSet<Text> splits = new TreeSet<>();
    int jump = MAX_DATA / numTablets;

    for (int r = jump; r < MAX_DATA; r += jump) {
      splits.add(new Text(row(r)));
    }

    createTable(client, tableName, service, splits);
  }

  public static void createTable(AccumuloClient client, String tableName, String service,
      SortedSet<Text> splits) throws Exception {
    Map<String,String> props =
        Map.of("table.compaction.dispatcher", SimpleCompactionDispatcher.class.getName(),
            "table.compaction.dispatcher.opts.service", service);
    NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props).withSplits(splits);

    client.tableOperations().create(tableName, ntc);

  }

  public static void writeData(AccumuloClient client, String table1)
      throws MutationsRejectedException, TableNotFoundException, AccumuloException,
      AccumuloSecurityException {
    try (BatchWriter bw = client.createBatchWriter(table1)) {
      for (int i = 0; i < MAX_DATA; i++) {
        Mutation m = new Mutation(row(i));
        m.put("", "", "" + i);
        bw.addMutation(m);
      }
    }

    client.tableOperations().flush(table1);
  }

  public static void verify(AccumuloClient client, String table1, int modulus)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    try (Scanner scanner = client.createScanner(table1)) {
      int count = 0;
      for (Entry<Key,Value> entry : scanner) {
        assertTrue(String.format("%s %s %d != 0", entry.getValue(), "%", modulus),
            Integer.parseInt(entry.getValue().toString()) % modulus == 0);
        count++;
      }

      int expectedCount = 0;
      for (int i = 0; i < MAX_DATA; i++) {
        if (i % modulus == 0)
          expectedCount++;
      }

      assertEquals(expectedCount, count);
    }
  }

  public static void stopProcesses(ProcessInfo... processes) throws Exception {
    for (ProcessInfo p : processes) {
      if (p != null) {
        Process proc = p.getProcess();
        if (proc.supportsNormalTermination()) {
          LOG.info("Stopping process {}", proc.pid());
          proc.destroyForcibly().waitFor();
        } else {
          LOG.info("Stopping process {} manually", proc.pid());
          new ProcessBuilder("kill", Long.toString(proc.pid())).start();
          proc.waitFor();
        }
      }
    }
  }

  public static void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {

    // ecomp writes from the TabletServer are not being written to the metadata
    // table, they are being queued up instead.
    Map<String,String> clProps = Maps.newHashMap();
    clProps.put(ClientProperty.BATCH_WRITER_LATENCY_MAX.getKey(), "2s");
    cfg.setClientProps(clProps);

    cfg.setProperty("tserver.compaction.major.service.cs1.planner",
        DefaultCompactionPlanner.class.getName());
    cfg.setProperty("tserver.compaction.major.service.cs1.planner.opts.executors",
        "[{'name':'all', 'type': 'external', 'queue': 'DCQ1'}]");
    cfg.setProperty("tserver.compaction.major.service.cs2.planner",
        DefaultCompactionPlanner.class.getName());
    cfg.setProperty("tserver.compaction.major.service.cs2.planner.opts.executors",
        "[{'name':'all', 'type': 'external','queue': 'DCQ2'}]");
    cfg.setProperty(Property.COMPACTION_COORDINATOR_FINALIZER_COMPLETION_CHECK_INTERVAL, "5s");
    cfg.setProperty(Property.COMPACTION_COORDINATOR_DEAD_COMPACTOR_CHECK_INTERVAL, "5s");
    cfg.setProperty(Property.COMPACTION_COORDINATOR_TSERVER_COMPACTION_CHECK_INTERVAL, "3s");
    cfg.setProperty(Property.COMPACTOR_PORTSEARCH, "true");
    cfg.setProperty(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE, "10");
    cfg.setProperty(Property.MANAGER_FATE_THREADPOOL_SIZE, "10");
    // use raw local file system so walogs sync and flush will work
    coreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  private static HttpRequest req = null;
  static {
    try {
      req = HttpRequest.newBuilder().GET().uri(new URI("http://localhost:9099/metrics")).build();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private static final HttpClient hc =
      HttpClient.newBuilder().version(Version.HTTP_1_1).followRedirects(Redirect.NORMAL).build();

  public static ExternalCompactionMetrics getCoordinatorMetrics() throws Exception {
    HttpResponse<String> res = hc.send(req, BodyHandlers.ofString());
    assertEquals(200, res.statusCode());
    String metrics = res.body();
    assertNotNull(metrics);
    return new Gson().fromJson(metrics, ExternalCompactionMetrics.class);
  }

  public static ProcessInfo startCoordinator(MiniAccumuloClusterImpl cluster,
      Class<? extends CompactionCoordinator> coord) throws IOException {
    ProcessInfo pi = cluster.exec(coord);
    if (TestCompactionCoordinator.class.isAssignableFrom(coord)) {
      // Wait for coordinator to start
      ExternalCompactionMetrics metrics = null;
      while (null == metrics) {
        try {
          metrics = getCoordinatorMetrics();
        } catch (Exception e) {
          UtilWaitThread.sleep(250);
        }
      }
    }
    return pi;
  }

}
