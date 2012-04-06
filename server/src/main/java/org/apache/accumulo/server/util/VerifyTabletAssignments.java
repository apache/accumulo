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

import java.io.PrintWriter;
import java.nio.ByteBuffer;
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

import jline.ConsoleReader;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.thrift.InitialMultiScan;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.MultiScanResult;
import org.apache.accumulo.core.data.thrift.TColumn;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;

public class VerifyTabletAssignments {
  
  public static void main(String[] args) throws Exception {
    Options opts = new Options();
    
    Option zooKeeperInstance = new Option("z", "zooKeeperInstance", true, "use a zookeeper instance with the given instance name and list of zoo hosts");
    zooKeeperInstance.setArgName("name hosts");
    zooKeeperInstance.setArgs(2);
    opts.addOption(zooKeeperInstance);
    
    Option usernameOption = new Option("u", "user", true, "username (required)");
    usernameOption.setArgName("user");
    usernameOption.setRequired(true);
    opts.addOption(usernameOption);
    
    Option passwOption = new Option("p", "password", true, "password (prompt for password if this option is missing)");
    passwOption.setArgName("pass");
    opts.addOption(passwOption);
    
    Option verboseOption = new Option("v", "verbose", false, "verbose mode (prints locations of tablets)");
    opts.addOption(verboseOption);
    
    CommandLine cl = null;
    String user = null;
    String passw = null;
    Instance instance = null;
    ConsoleReader reader = new ConsoleReader();
    try {
      cl = new BasicParser().parse(opts, args);
      
      if (cl.hasOption(zooKeeperInstance.getOpt()) && cl.getOptionValues(zooKeeperInstance.getOpt()).length != 2)
        throw new MissingArgumentException(zooKeeperInstance);
      
      user = cl.getOptionValue(usernameOption.getOpt());
      passw = cl.getOptionValue(passwOption.getOpt());
      
      if (cl.hasOption(zooKeeperInstance.getOpt())) {
        String[] zkOpts = cl.getOptionValues(zooKeeperInstance.getOpt());
        instance = new ZooKeeperInstance(zkOpts[0], zkOpts[1]);
      } else {
        instance = HdfsZooInstance.getInstance();
      }
      
      if (passw == null)
        passw = reader.readLine("Enter current password for '" + user + "'@'" + instance.getInstanceName() + "': ", '*');
      if (passw == null) {
        reader.printNewline();
        return;
      } // user canceled
      
      if (cl.getArgs().length != 0)
        throw new ParseException("Unrecognized arguments: " + cl.getArgList());
      
    } catch (ParseException e) {
      PrintWriter pw = new PrintWriter(System.err);
      new HelpFormatter().printHelp(pw, Integer.MAX_VALUE, "accumulo " + VerifyTabletAssignments.class.getName(), null, opts, 2, 5, null, true);
      pw.flush();
      System.exit(1);
    }
    
    Connector conn = instance.getConnector(user, passw.getBytes());
    ServerConfiguration conf = new ServerConfiguration(instance);
    for (String table : conn.tableOperations().list())
      checkTable(conf.getConfiguration(), user, passw, table, null, cl.hasOption(verboseOption.getOpt()));
    
  }
  
  private static void checkTable(final AccumuloConfiguration conf, final String user, final String pass, String table, HashSet<KeyExtent> check, boolean verbose)
      throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException, InterruptedException {
    
    if (check == null)
      System.out.println("Checking table " + table);
    else
      System.out.println("Checking table " + table + " again, failures " + check.size());
    
    Map<KeyExtent,String> locations = new TreeMap<KeyExtent,String>();
    SortedSet<KeyExtent> tablets = new TreeSet<KeyExtent>();
    
    MetadataTable.getEntries(HdfsZooInstance.getInstance(),
        new AuthInfo(user, ByteBuffer.wrap(pass.getBytes()), HdfsZooInstance.getInstance().getInstanceID()), table, false, locations, tablets);
    
    final HashSet<KeyExtent> failures = new HashSet<KeyExtent>();
    
    for (KeyExtent keyExtent : tablets)
      if (!locations.containsKey(keyExtent))
        System.out.println(" Tablet " + keyExtent + " has no location");
      else if (verbose)
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
    
    for (final Entry<String,List<KeyExtent>> entry : extentsPerServer.entrySet()) {
      Runnable r = new Runnable() {
        
        @Override
        public void run() {
          try {
            checkTabletServer(conf, user, ByteBuffer.wrap(pass.getBytes()), entry, failures);
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
      checkTable(conf, user, pass, table, failures, verbose);
  }
  
  private static void checkFailures(String server, HashSet<KeyExtent> failures, MultiScanResult scanResult) {
    for (TKeyExtent tke : scanResult.failures.keySet()) {
      KeyExtent ke = new KeyExtent(tke);
      System.out.println(" Tablet " + ke + " failed at " + server);
      failures.add(ke);
    }
  }
  
  private static void checkTabletServer(AccumuloConfiguration conf, final String user, final ByteBuffer pass, Entry<String,List<KeyExtent>> entry,
      HashSet<KeyExtent> failures)
      throws ThriftSecurityException, TException, NoSuchScanIDException {
    TabletClientService.Iface client = ThriftUtil.getTServerClient(entry.getKey(), conf);
    
    AuthInfo st = new AuthInfo(user, pass, HdfsZooInstance.getInstance().getInstanceID());
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
    
    Map<String,Map<String,String>> emptyMapSMapSS = Collections.emptyMap();
    List<IterInfo> emptyListIterInfo = Collections.emptyList();
    List<TColumn> emptyListColumn = Collections.emptyList();
    InitialMultiScan is = client.startMultiScan(null, st, batch, emptyListColumn, emptyListIterInfo, emptyMapSMapSS, Constants.NO_AUTHS.getAuthorizationsBB(),
        false);
    if (is.result.more) {
      MultiScanResult result = client.continueMultiScan(null, is.scanID);
      checkFailures(entry.getKey(), failures, result);
      
      while (result.more) {
        result = client.continueMultiScan(null, is.scanID);
        checkFailures(entry.getKey(), failures, result);
      }
    }
    
    client.closeMultiScan(null, is.scanID);
    
    ThriftUtil.returnClient((TServiceClient) client);
  }
}
