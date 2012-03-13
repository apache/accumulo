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
package org.apache.accumulo.server.test;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.io.Text;

public class TestMultiTableIngest {
  
  private static ArrayList<String> tableNames = new ArrayList<String>();
  
  private static Option usernameOpt = new Option("username", true, "username");
  private static Option passwordOpt = new Option("password", true, "password");
  private static Option readonlyOpt = new Option("readonly", false, "read only");
  private static Option tablesOpt = new Option("tables", true, "number of tables to create");
  private static Option countOpt = new Option("count", true, "number of entries to create");
  
  private static Options opts = new Options();
  
  static {
    opts.addOption(usernameOpt);
    opts.addOption(passwordOpt);
    opts.addOption(readonlyOpt);
    opts.addOption(tablesOpt);
    opts.addOption(countOpt);
  }
  
  // root user is needed for tests
  private static String user;
  private static String password;
  private static boolean readOnly = false;
  private static int count = 10000;
  private static int tables = 5;
  
  private static void readBack(Connector conn, int last) throws Exception {
    int i = 0;
    for (String table : tableNames) {
      Scanner scanner = conn.createScanner(table, Constants.NO_AUTHS);
      int count = i;
      for (Entry<Key,Value> elt : scanner) {
        String expected = String.format("%05d", count);
        assert (elt.getKey().getRow().toString().equals(expected));
        count += tableNames.size();
      }
      i++;
    }
    assert (last == count);
  }
  
  public static void main(String[] args) throws Exception {
    
    Parser p = new BasicParser();
    CommandLine cl = null;
    
    try {
      cl = p.parse(opts, args);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
    String[] rargs = cl.getArgs();
    if (rargs.length != 0) {
      HelpFormatter hf = new HelpFormatter();
      hf.printHelp("", opts);
    }
    count = Integer.parseInt(cl.getOptionValue(countOpt.getOpt(), "10000"));
    tables = Integer.parseInt(cl.getOptionValue(tablesOpt.getOpt(), "5"));
    readOnly = cl.hasOption(readonlyOpt.getOpt());
    user = cl.getOptionValue(usernameOpt.getOpt(), "root");
    password = cl.getOptionValue(passwordOpt.getOpt(), "secret");
    
    // create the test table within accumulo
    Connector connector;
    try {
      connector = HdfsZooInstance.getInstance().getConnector(user, password.getBytes());
    } catch (AccumuloException e) {
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      throw new RuntimeException(e);
    }
    for (int i = 0; i < tables; i++) {
      tableNames.add(String.format("test_%04d", i));
    }
    
    if (!readOnly) {
      for (String table : tableNames)
        connector.tableOperations().create(table);
      
      MultiTableBatchWriter b;
      try {
        b = connector.createMultiTableBatchWriter(10000000, 1000000, 10);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      
      // populate
      for (int i = 0; i < count; i++) {
        Mutation m = new Mutation(new Text(String.format("%05d", i)));
        m.put(new Text("col" + Integer.toString((i % 3) + 1)), new Text("qual"), new Value("junk".getBytes()));
        b.getBatchWriter(tableNames.get(i % tableNames.size())).addMutation(m);
      }
      try {
        b.close();
      } catch (MutationsRejectedException e) {
        throw new RuntimeException(e);
      }
    }
    try {
      readBack(connector, count);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
}
