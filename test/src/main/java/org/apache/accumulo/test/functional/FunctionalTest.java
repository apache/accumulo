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
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.CredentialHelper;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public abstract class FunctionalTest {
  
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
  
  private AuthenticationToken token = null;
  private String instanceName = "";
  private String principal = "";
  
  protected void setPrincipal(String princ) {
    this.principal = princ;
  }
  
  protected String getPrincipal() {
    return principal;
  }
  
  protected void setToken(AuthenticationToken token) {
    this.token = token;
  }
  
  protected AuthenticationToken getToken() {
    return token;
  }
  
  protected Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    return getInstance().getConnector(getPrincipal(), getToken());
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
    Collection<Text> splits = getConnector().tableOperations().listSplits(table);
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
    Constants.METADATA_PREV_ROW_COLUMN.fetch(scanner);
    
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
  
  static class Opts extends ClientOpts {
    @Parameter(names = "--classname", required = true, description = "name of the class under test")
    String classname = null;
    
    @Parameter(names = "--opt", required = true, description = "the options for test")
    String opt = null;
  }
  
  public static void main(String[] args) throws Exception {
    CredentialHelper.create("", new PasswordToken(new byte[0]), "");
    Opts opts = new Opts();
    opts.parseArgs(FunctionalTest.class.getName(), args);
    
    Class<? extends FunctionalTest> testClass = AccumuloVFSClassLoader.loadClass(opts.classname, FunctionalTest.class);
    FunctionalTest fTest = testClass.newInstance();
    
    // fTest.setMaster(master);
    fTest.setInstanceName(opts.instance);
    fTest.setPrincipal(opts.principal);
    fTest.setToken(opts.getToken());
    
    if (opts.opt.equals("getConfig")) {
      Map<String,String> iconfig = fTest.getInitialConfig();
      System.out.println("{");
      for (Entry<String,String> entry : iconfig.entrySet()) {
        System.out.println("'" + entry.getKey() + "':'" + entry.getValue() + "',");
      }
      System.out.println("}");
    } else if (opts.opt.equals("setup")) {
      fTest.setup();
    } else if (opts.opt.equals("run")) {
      fTest.run();
    } else if (opts.opt.equals("cleanup")) {
      fTest.cleanup();
    } else {
      printHelpAndExit("Unknown option: " + opts.opt);
    }
    
  }
  
  static void printHelpAndExit(String message) {
    System.out.println(message);
    new JCommander(new Opts()).usage();
    System.exit(1);
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
