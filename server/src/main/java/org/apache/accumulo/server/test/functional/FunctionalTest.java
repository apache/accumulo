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
package org.apache.accumulo.server.test.functional;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public abstract class FunctionalTest {
  private static Options opts;
  private static Option masterOpt;
  private static Option passwordOpt;
  private static Option usernameOpt;
  private static Option instanceNameOpt;
  
  static {
    usernameOpt = new Option("u", "username", true, "username");
    passwordOpt = new Option("p", "password", true, "password");
    masterOpt = new Option("m", "master", true, "master");
    instanceNameOpt = new Option("i", "instanceName", true, "instance name");
    
    opts = new Options();
    
    opts.addOption(usernameOpt);
    opts.addOption(passwordOpt);
    opts.addOption(masterOpt);
    opts.addOption(instanceNameOpt);
  }
  
  public static Map<String,String> parseConfig(String... perTableConfigs) {
    
    TreeMap<String,String> config = new TreeMap<String,String>();
    
    for (String line : perTableConfigs) {
      String[] splitLine = line.split("=");
      if (splitLine.length == 1 && line.endsWith("="))
        config.put(splitLine[0], "");
      else
        config.put(splitLine[0], splitLine[1]);
    }
    
    return config;
    
  }
  
  public static class TableSetup {
    private String tableName;
    private Map<String,String> perTableConfigs;
    private SortedSet<Text> splitPoints;
    
    public TableSetup(String tableName) {
      this.tableName = tableName;
    }
    
    public TableSetup(String tableName, Map<String,String> perTableConfigs) {
      this.tableName = tableName;
      this.perTableConfigs = perTableConfigs;
    }
    
    public TableSetup(String tableName, Map<String,String> perTableConfigs, SortedSet<Text> splitPoints) {
      this.tableName = tableName;
      this.perTableConfigs = perTableConfigs;
      this.splitPoints = splitPoints;
    }
    
    public TableSetup(String tableName, SortedSet<Text> splitPoints) {
      this.tableName = tableName;
      this.splitPoints = splitPoints;
    }
    
    public TableSetup(String tableName, String... splitPoints) {
      this.tableName = tableName;
      
      this.splitPoints = new TreeSet<Text>();
      for (String split : splitPoints) {
        this.splitPoints.add(new Text(split));
      }
    }
    
  }
  
  private String master = "";
  private String username = "";
  private String password = "";
  private String instanceName = "";
  
  protected void setMaster(String master) {
    this.master = master;
  }
  
  protected String getMaster() {
    return master;
  }
  
  protected void setUsername(String username) {
    this.username = username;
  }
  
  protected String getUsername() {
    return username;
  }
  
  protected void setPassword(String password) {
    this.password = password;
  }
  
  protected String getPassword() {
    return password;
  }
  
  protected Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    return getInstance().getConnector(username, password.getBytes());
  }
  
  protected Instance getInstance() {
    return new ZooKeeperInstance(getInstanceName(), ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_ZK_HOST));
  }
  
  protected void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }
  
  private String getInstanceName() {
    return instanceName;
  }
  
  protected AuthInfo getCredentials() {
    return new AuthInfo(getUsername(), ByteBuffer.wrap(getPassword().getBytes()), getInstance().getInstanceID());
  }
  
  public abstract Map<String,String> getInitialConfig();
  
  public abstract List<TableSetup> getTablesToCreate();
  
  public abstract void run() throws Exception;
  
  public abstract void cleanup() throws Exception;
  
  public void setup() throws Exception {
    Connector conn = getConnector();
    
    List<TableSetup> ttcl = getTablesToCreate();
    
    for (TableSetup tableSetup : ttcl) {
      if (tableSetup.splitPoints != null) {
        conn.tableOperations().create(tableSetup.tableName);
        conn.tableOperations().addSplits(tableSetup.tableName, tableSetup.splitPoints);
      } else {
        conn.tableOperations().create(tableSetup.tableName);
      }
      
      if (tableSetup.perTableConfigs != null) {
        for (Entry<String,String> entry : tableSetup.perTableConfigs.entrySet()) {
          conn.tableOperations().setProperty(tableSetup.tableName, entry.getKey(), entry.getValue());
        }
      }
    }
  }
  
  /**
   * A utility method for use by functional test that ensures a tables has between min and max split points inclusive. If not an exception is thrown.
   * 
   */
  
  protected void checkSplits(String table, int min, int max) throws Exception {
    Collection<Text> splits = getConnector().tableOperations().getSplits(table);
    if (splits.size() < min || splits.size() > max) {
      throw new Exception("# of table splits points out of range, #splits=" + splits.size() + " table=" + table + " min=" + min + " max=" + max);
    }
  }
  
  /**
   * A utility function that checks that each tablet has an expected number of rfiles.
   * 
   */
  
  protected void checkRFiles(String tableName, int minTablets, int maxTablets, int minRFiles, int maxRFiles) throws Exception {
    Scanner scanner = getConnector().createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
    String tableId = Tables.getNameToIdMap(getInstance()).get(tableName);
    scanner.setRange(new Range(new Text(tableId + ";"), true, new Text(tableId + "<"), true));
    scanner.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
    ColumnFQ.fetch(scanner, Constants.METADATA_PREV_ROW_COLUMN);
    
    HashMap<Text,Integer> tabletFileCounts = new HashMap<Text,Integer>();
    
    for (Entry<Key,Value> entry : scanner) {
      
      Text row = entry.getKey().getRow();
      
      Integer count = tabletFileCounts.get(row);
      if (count == null)
        count = 0;
      if (entry.getKey().getColumnFamily().equals(Constants.METADATA_DATAFILE_COLUMN_FAMILY)) {
        count = count + 1;
      }
      
      tabletFileCounts.put(row, count);
    }
    
    if (tabletFileCounts.size() < minTablets || tabletFileCounts.size() > maxTablets) {
      throw new Exception("Did not find expected number of tablets " + tabletFileCounts.size());
    }
    
    Set<Entry<Text,Integer>> es = tabletFileCounts.entrySet();
    for (Entry<Text,Integer> entry : es) {
      if (entry.getValue() > maxRFiles || entry.getValue() < minRFiles) {
        throw new Exception("tablet " + entry.getKey() + " has " + entry.getValue() + " map files");
      }
    }
  }
  
  protected void bulkImport(FileSystem fs, String table, String dir) throws Exception {
    String failDir = dir + "_failures";
    Path failPath = new Path(failDir);
    fs.delete(failPath, true);
    fs.mkdirs(failPath);
    
    getConnector().tableOperations().importDirectory(table, dir, failDir, false);
    
    if (fs.listStatus(failPath).length > 0) {
      throw new Exception("Some files failed to bulk import");
    }
    
  }
  
  public static void main(String[] args) throws Exception {
    Parser p = new BasicParser();
    
    CommandLine cl = null;
    try {
      cl = p.parse(opts, args);
    } catch (ParseException e) {
      System.out.println("Parse Exception, exiting.");
      return;
    }
    
    String master = cl.getOptionValue(masterOpt.getOpt(), "localhost");
    String username = cl.getOptionValue(usernameOpt.getOpt(), "root");
    String password = cl.getOptionValue(passwordOpt.getOpt(), "secret");
    String instanceName = cl.getOptionValue(instanceNameOpt.getOpt(), "FuncTest");
    
    String remainingArgs[] = cl.getArgs();
    String clazz = remainingArgs[0];
    String opt = remainingArgs[1];
    
    Class<? extends FunctionalTest> testClass = AccumuloClassLoader.loadClass(clazz, FunctionalTest.class);
    FunctionalTest fTest = testClass.newInstance();
    
    fTest.setMaster(master);
    fTest.setUsername(username);
    fTest.setPassword(password);
    fTest.setInstanceName(instanceName);
    
    if (opt.equals("getConfig")) {
      Map<String,String> iconfig = fTest.getInitialConfig();
      System.out.println("{");
      for (Entry<String,String> entry : iconfig.entrySet()) {
        System.out.println("'" + entry.getKey() + "':'" + entry.getValue() + "',");
      }
      System.out.println("}");
    } else if (opt.equals("setup")) {
      fTest.setup();
    } else if (opt.equals("run")) {
      fTest.run();
    } else if (opt.equals("cleanup")) {
      fTest.cleanup();
    }
    
  }
  
  static Mutation nm(String row, String cf, String cq, Value value) {
    Mutation m = new Mutation(new Text(row));
    m.put(new Text(cf), new Text(cq), value);
    return m;
  }
  
  static Mutation nm(String row, String cf, String cq, String value) {
    return nm(row, cf, cq, new Value(value.getBytes()));
  }
}
