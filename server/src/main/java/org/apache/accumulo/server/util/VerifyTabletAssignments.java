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
package org.apache.accumulo.server.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.thrift.InitialMultiScan;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.MultiScanResult;
import org.apache.accumulo.core.data.thrift.TColumn;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.accumulo.core.security.CredentialHelper;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.accumulo.trace.thrift.TInfo;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;

import com.beust.jcommander.Parameter;

public class VerifyTabletAssignments {
  
  static class Opts extends ClientOpts {
    @Parameter(names = {"-v", "--verbose"}, description = "verbose mode (prints locations of tablets)")
    boolean verbose = false;
  }
  
  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(VerifyTabletAssignments.class.getName(), args);
    
    Connector conn = opts.getConnector();
    for (String table : conn.tableOperations().list())
      checkTable(opts, table, null);
    
  }
  
  private static void checkTable(final Opts opts, String tableName, HashSet<KeyExtent> check) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, InterruptedException {
    
    if (check == null)
      System.out.println("Checking table " + tableName);
    else
      System.out.println("Checking table " + tableName + " again, failures " + check.size());
    
    Map<KeyExtent,String> locations = new TreeMap<KeyExtent,String>();
    SortedSet<KeyExtent> tablets = new TreeSet<KeyExtent>();
    
    Connector conn = opts.getConnector();
    Instance inst = conn.getInstance();
    MetadataTable.getEntries(conn.getInstance(), CredentialHelper.create(opts.principal, opts.getToken(), opts.instance), tableName, false,
        locations, tablets);
    
    final HashSet<KeyExtent> failures = new HashSet<KeyExtent>();
    
    for (KeyExtent keyExtent : tablets)
      if (!locations.containsKey(keyExtent))
        System.out.println(" Tablet " + keyExtent + " has no location");
      else if (opts.verbose)
        System.out.println(" Tablet " + keyExtent + " is located at " + locations.get(keyExtent));
    
    Map<String,List<KeyExtent>> extentsPerServer = new TreeMap<String,List<KeyExtent>>();
    
    for (Entry<KeyExtent,String> entry : locations.entrySet()) {
      List<KeyExtent> extentList = extentsPerServer.get(entry.getValue());
      if (extentList == null) {
        extentList = new ArrayList<KeyExtent>();
        extentsPerServer.put(entry.getValue(), extentList);
      }
      
      if (check == null || check.contains(entry.getKey()))
        extentList.add(entry.getKey());
    }
    
    ExecutorService tp = Executors.newFixedThreadPool(20);
    final ServerConfiguration conf = new ServerConfiguration(inst);
    for (final Entry<String,List<KeyExtent>> entry : extentsPerServer.entrySet()) {
      Runnable r = new Runnable() {
        
        @Override
        public void run() {
          try {
            checkTabletServer(conf.getConfiguration(), CredentialHelper.create(opts.principal, opts.getToken(), opts.instance), entry, failures);
          } catch (Exception e) {
            System.err.println("Failure on ts " + entry.getKey() + " " + e.getMessage());
            e.printStackTrace();
            failures.addAll(entry.getValue());
          }
        }
        
      };
      
      tp.execute(r);
    }
    
    tp.shutdown();
    
    while (!tp.awaitTermination(1, TimeUnit.HOURS)) {}
    
    if (failures.size() > 0)
      checkTable(opts, tableName, failures);
  }
  
  private static void checkFailures(String server, HashSet<KeyExtent> failures, MultiScanResult scanResult) {
    for (TKeyExtent tke : scanResult.failures.keySet()) {
      KeyExtent ke = new KeyExtent(tke);
      System.out.println(" Tablet " + ke + " failed at " + server);
      failures.add(ke);
    }
  }
  
  private static void checkTabletServer(AccumuloConfiguration conf, TCredentials st, Entry<String,List<KeyExtent>> entry,
      HashSet<KeyExtent> failures) throws ThriftSecurityException, TException, NoSuchScanIDException {
    TabletClientService.Iface client = ThriftUtil.getTServerClient(entry.getKey(), conf);
    
    Map<TKeyExtent,List<TRange>> batch = new TreeMap<TKeyExtent,List<TRange>>();
    
    for (KeyExtent keyExtent : entry.getValue()) {
      Text row = keyExtent.getEndRow();
      Text row2 = null;
      
      if (row == null) {
        row = keyExtent.getPrevEndRow();
        
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
    TInfo tinfo = Tracer.traceInfo();
    Map<String,Map<String,String>> emptyMapSMapSS = Collections.emptyMap();
    List<IterInfo> emptyListIterInfo = Collections.emptyList();
    List<TColumn> emptyListColumn = Collections.emptyList();
    InitialMultiScan is = client.startMultiScan(tinfo, st, batch, emptyListColumn, emptyListIterInfo, emptyMapSMapSS, Constants.NO_AUTHS.getAuthorizationsBB(),
        false);
    if (is.result.more) {
      MultiScanResult result = client.continueMultiScan(tinfo, is.scanID);
      checkFailures(entry.getKey(), failures, result);
      
      while (result.more) {
        result = client.continueMultiScan(tinfo, is.scanID);
        checkFailures(entry.getKey(), failures, result);
      }
    }
    
    client.closeMultiScan(tinfo, is.scanID);
    
    ThriftUtil.returnClient((TServiceClient) client);
  }
}
