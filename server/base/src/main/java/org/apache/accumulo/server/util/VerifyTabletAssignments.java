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
package org.apache.accumulo.server.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.InitialMultiScan;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.MultiScanResult;
import org.apache.accumulo.core.dataImpl.thrift.TColumn;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.metadata.MetadataServicer;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.thrift.TabletScanClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class VerifyTabletAssignments {
  private static final Logger log = LoggerFactory.getLogger(VerifyTabletAssignments.class);

  public static void execute(Properties clientProps, boolean verbose) throws Exception {
    Span span = TraceUtil.startSpan(VerifyTabletAssignments.class, "main");
    try (Scope scope = span.makeCurrent()) {
      try (AccumuloClient client = Accumulo.newClient().from(clientProps).build()) {
        for (String table : client.tableOperations().list()) {
          checkTable((ClientContext) client, verbose, table, null);
        }
      } finally {
        span.end();
      }
    }
  }

  private static void checkTable(final ClientContext context, final boolean verbose,
      String tableName, HashSet<KeyExtent> check) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException, InterruptedException {

    if (check == null) {
      System.out.println("Checking table " + tableName);
    } else {
      System.out.println("Checking table " + tableName + " again, failures " + check.size());
    }

    TreeMap<KeyExtent,String> tabletLocations = new TreeMap<>();

    TableId tableId = context.getTableNameToIdMap().get(tableName);
    MetadataServicer.forTableId(context, tableId).getTabletLocations(tabletLocations);

    final HashSet<KeyExtent> failures = new HashSet<>();

    Map<HostAndPort,List<KeyExtent>> extentsPerServer = new TreeMap<>();

    for (Entry<KeyExtent,String> entry : tabletLocations.entrySet()) {
      KeyExtent keyExtent = entry.getKey();
      String loc = entry.getValue();
      if (loc == null) {
        System.out.println(" Tablet " + keyExtent + " has no location");
      } else if (verbose) {
        System.out.println(" Tablet " + keyExtent + " is located at " + loc);
      }

      if (loc != null) {
        final HostAndPort parsedLoc = HostAndPort.fromString(loc);
        List<KeyExtent> extentList =
            extentsPerServer.computeIfAbsent(parsedLoc, k -> new ArrayList<>());

        if (check == null || check.contains(keyExtent)) {
          extentList.add(keyExtent);
        }
      }
    }

    ExecutorService tp = ThreadPools.getServerThreadPools().getPoolBuilder("CheckTabletServer")
        .numCoreThreads(20).build();

    for (final Entry<HostAndPort,List<KeyExtent>> entry : extentsPerServer.entrySet()) {
      Runnable r = () -> {
        try {
          checkTabletServer(context, entry, failures);
        } catch (Exception e) {
          log.error("Failure on tablet server '" + entry.getKey() + ".", e);
          failures.addAll(entry.getValue());
        }
      };

      tp.execute(r);
    }

    tp.shutdown();

    while (!tp.awaitTermination(1, TimeUnit.HOURS)) {}

    if (!failures.isEmpty()) {
      checkTable(context, verbose, tableName, failures);
    }
  }

  private static void checkFailures(HostAndPort server, HashSet<KeyExtent> failures,
      MultiScanResult scanResult) {
    for (TKeyExtent tke : scanResult.failures.keySet()) {
      KeyExtent ke = KeyExtent.fromThrift(tke);
      System.out.println(" Tablet " + ke + " failed at " + server);
      failures.add(ke);
    }
  }

  private static void checkTabletServer(ClientContext context,
      Entry<HostAndPort,List<KeyExtent>> entry, HashSet<KeyExtent> failures) throws TException {
    TabletScanClientService.Iface client =
        ThriftUtil.getClient(ThriftClientTypes.TABLET_SCAN, entry.getKey(), context);

    Map<TKeyExtent,List<TRange>> batch = new TreeMap<>();

    for (KeyExtent keyExtent : entry.getValue()) {
      Text row = keyExtent.endRow();
      Text row2 = null;

      if (row == null) {
        row = keyExtent.prevEndRow();

        if (row != null) {
          row = new Text(row);
          row.append(new byte[] {'a'}, 0, 1);
        } else {
          row = new Text("1234567890");
        }

        row2 = new Text(row);
        row2.append(new byte[] {'!'}, 0, 1);
      } else {
        row = new Text(row);
        row2 = new Text(row);

        row.getBytes()[row.getLength() - 1] = (byte) (row.getBytes()[row.getLength() - 1] - 1);
      }

      Range r = new Range(row, true, row2, false);
      batch.put(keyExtent.toThrift(), Collections.singletonList(r.toThrift()));
    }
    TInfo tinfo = TraceUtil.traceInfo();
    Map<String,Map<String,String>> emptyMapSMapSS = Collections.emptyMap();
    List<IterInfo> emptyListIterInfo = Collections.emptyList();
    List<TColumn> emptyListColumn = Collections.emptyList();
    InitialMultiScan is = client.startMultiScan(tinfo, context.rpcCreds(), batch, emptyListColumn,
        emptyListIterInfo, emptyMapSMapSS, Authorizations.EMPTY.getAuthorizationsBB(), false, null,
        0L, null, null, 0L);
    if (is.result.more) {
      MultiScanResult result = client.continueMultiScan(tinfo, is.scanID, 0L);
      checkFailures(entry.getKey(), failures, result);

      while (result.more) {
        result = client.continueMultiScan(tinfo, is.scanID, 0L);
        checkFailures(entry.getKey(), failures, result);
      }
    }

    client.closeMultiScan(tinfo, is.scanID);

    ThriftUtil.returnClient((TServiceClient) client, context);
  }
}
