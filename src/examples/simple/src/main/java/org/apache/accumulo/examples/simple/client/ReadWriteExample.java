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
package org.apache.accumulo.examples.simple.client;

import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ReadWriteExample {
  // defaults
  private static final String DEFAULT_INSTANCE_NAME = "test";
  private static final String DEFAULT_ZOOKEEPERS = "localhost";
  private static final String DEFAULT_AUTHS = "LEVEL1,GROUP1";
  private static final String DEFAULT_TABLE_NAME = "test";
  
  // options
  private Option instanceOpt = new Option("i", "instance", true, "instance name");
  private Option zooKeepersOpt = new Option("z", "zooKeepers", true, "zoo keepers");
  
  private Option usernameOpt = new Option("u", "user", true, "user name");
  private Option passwordOpt = new Option("p", "password", true, "password");
  private Option scanAuthsOpt = new Option("s", "scanauths", true, "comma-separated scan authorizations");
  
  private Option tableNameOpt = new Option("t", "table", true, "table name");
  private Option createtableOpt = new Option("C", "createtable", false, "create table before doing anything");
  private Option deletetableOpt = new Option("D", "deletetable", false, "delete table when finished");
  
  private Option createEntriesOpt = new Option("e", "create", false, "create entries before any deletes");
  private Option deleteEntriesOpt = new Option("d", "delete", false, "delete entries after any creates");
  private Option readEntriesOpt = new Option("r", "read", false, "read entries after any creates/deletes");
  
  private Option debugOpt = new Option("dbg", "debug", false, "enable debugging");
  
  private Options opts;
  private CommandLine cl;
  private Connector conn;
  
  // hidden constructor
  private ReadWriteExample() {}
  
  // setup
  private void configure(String[] args) throws ParseException, AccumuloException, AccumuloSecurityException {
    usernameOpt.setRequired(true);
    passwordOpt.setRequired(true);
    opts = new Options();
    addOptions(instanceOpt, zooKeepersOpt, usernameOpt, passwordOpt, scanAuthsOpt, tableNameOpt, createtableOpt, deletetableOpt, createEntriesOpt,
        deleteEntriesOpt, readEntriesOpt, debugOpt);
    
    // parse command line
    cl = new BasicParser().parse(opts, args);
    if (cl.getArgs().length != 0)
      throw new ParseException("unrecognized options " + cl.getArgList());
    
    // optionally enable debugging
    if (hasOpt(debugOpt))
      Logger.getLogger(Constants.CORE_PACKAGE_NAME).setLevel(Level.TRACE);
    
    Instance inst = new ZooKeeperInstance(getOpt(instanceOpt, DEFAULT_INSTANCE_NAME), getOpt(zooKeepersOpt, DEFAULT_ZOOKEEPERS));
    conn = inst.getConnector(getRequiredOpt(usernameOpt), getRequiredOpt(passwordOpt).getBytes());
  }
  
  // for setup
  private void addOptions(Option... addOpts) {
    for (Option opt : addOpts)
      opts.addOption(opt);
  }
  
  // for checking for and getting options
  private boolean hasOpt(Option opt) {
    return cl.hasOption(opt.getOpt());
  }
  
  private String getRequiredOpt(Option opt) {
    return getOpt(opt, null);
  }
  
  private String getOpt(Option opt, String defaultValue) {
    return cl.getOptionValue(opt.getOpt(), defaultValue);
  }
  
  // for usage
  private void printHelp() {
    HelpFormatter hf = new HelpFormatter();
    instanceOpt.setArgName("name");
    zooKeepersOpt.setArgName("hosts");
    usernameOpt.setArgName("user");
    passwordOpt.setArgName("pass");
    scanAuthsOpt.setArgName("scanauths");
    tableNameOpt.setArgName("name");
    hf.printHelp("accumulo accumulo.examples.client.ReadWriteExample", opts, true);
  }
  
  private void execute() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, MutationsRejectedException {
    // create table
    if (hasOpt(createtableOpt)) {
      SortedSet<Text> partitionKeys = new TreeSet<Text>();
      for (int i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++)
        partitionKeys.add(new Text(new byte[] {(byte) i}));
      conn.tableOperations().create(getOpt(tableNameOpt, DEFAULT_TABLE_NAME));
      conn.tableOperations().addSplits(getOpt(tableNameOpt, DEFAULT_TABLE_NAME), partitionKeys);
    }
    
    // create entries
    if (hasOpt(createEntriesOpt))
      createEntries(false);
    
    // delete entries
    if (hasOpt(deleteEntriesOpt))
      createEntries(true);
    
    // read entries
    if (hasOpt(readEntriesOpt)) {
      // Note that the user needs to have the authorizations for the specified scan authorizations
      // by an administrator first
      Authorizations scanauths = new Authorizations(getOpt(scanAuthsOpt, DEFAULT_AUTHS).split(","));
      
      Scanner scanner = conn.createScanner(getOpt(tableNameOpt, DEFAULT_TABLE_NAME), scanauths);
      for (Entry<Key,Value> entry : scanner)
        System.out.println(entry.getKey().toString() + " -> " + entry.getValue().toString());
    }
    
    // delete table
    if (hasOpt(deletetableOpt))
      conn.tableOperations().delete(getOpt(tableNameOpt, DEFAULT_TABLE_NAME));
  }
  
  private void createEntries(boolean delete) throws AccumuloException, TableNotFoundException, MutationsRejectedException {
    BatchWriter writer = conn.createBatchWriter(getOpt(tableNameOpt, DEFAULT_TABLE_NAME), 10000, Long.MAX_VALUE, 1);
    ColumnVisibility cv = new ColumnVisibility(DEFAULT_AUTHS.replace(',', '|'));
    
    Text cf = new Text("datatypes");
    Text cq = new Text("xml");
    byte[] row = {'h', 'e', 'l', 'l', 'o', '\0'};
    byte[] value = {'w', 'o', 'r', 'l', 'd', '\0'};
    
    for (int i = 0; i < 10; i++) {
      row[row.length - 1] = (byte) i;
      Mutation m = new Mutation(new Text(row));
      if (delete) {
        m.putDelete(cf, cq, cv);
      } else {
        value[value.length - 1] = (byte) i;
        m.put(cf, cq, cv, new Value(value));
      }
      writer.addMutation(m);
    }
    writer.close();
  }
  
  public static void main(String[] args) throws Exception {
    ReadWriteExample rwe = new ReadWriteExample();
    
    try {
      rwe.configure(args);
      rwe.execute();
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      rwe.printHelp();
      System.exit(1);
    }
  }
}
