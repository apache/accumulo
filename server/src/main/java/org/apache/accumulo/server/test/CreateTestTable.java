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

import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
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

public class CreateTestTable {
  private static Option usernameOpt;
  private static Option passwordOpt;
  private static Option readonlyOpt;
  
  private static Options opts;
  
  // root user is needed for tests
  private static String user;
  private static String password;
  private static boolean readOnly = false;
  private static int count = 10000;
  
  private static void readBack(Connector conn, int last) throws Exception {
    Scanner scanner = conn.createScanner("mrtest1", Constants.NO_AUTHS);
    int count = 0;
    for (Entry<Key,Value> elt : scanner) {
      String expected = String.format("%05d", count);
      assert (elt.getKey().getRow().toString().equals(expected));
      count++;
    }
    assert (last == count);
  }
  
  public static void setupOptions() {
    usernameOpt = new Option("username", "username", true, "username");
    passwordOpt = new Option("password", "password", true, "password");
    readonlyOpt = new Option("readonly", "readonly", false, "read only");
    
    opts = new Options();
    
    opts.addOption(usernameOpt);
    opts.addOption(passwordOpt);
    opts.addOption(readonlyOpt);
  }
  
  public static void main(String[] args) throws Exception {
    setupOptions();
    
    Parser p = new BasicParser();
    CommandLine cl = null;
    
    try {
      cl = p.parse(opts, args);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
    String[] rargs = cl.getArgs();
    if (rargs.length != 1) {
      HelpFormatter hf = new HelpFormatter();
      hf.printHelp(" <count> ", opts);
    }
    count = Integer.parseInt(rargs[0]);
    readOnly = cl.hasOption(readonlyOpt.getOpt());
    user = cl.getOptionValue(usernameOpt.getOpt(), "root");
    password = cl.getOptionValue(passwordOpt.getOpt(), "secret");
    
    // create the test table within accumulo
    String table = "mrtest1";
    Connector connector;
    
    connector = HdfsZooInstance.getInstance().getConnector(user, password.getBytes());
    
    if (!readOnly) {
      TreeSet<Text> keys = new TreeSet<Text>();
      for (int i = 0; i < count / 100; i++) {
        keys.add(new Text(String.format("%05d", i * 100)));
      }
      
      // presplit
      connector.tableOperations().create(table);
      connector.tableOperations().addSplits(table, keys);
      BatchWriter b = connector.createBatchWriter(table, 10000000l, 1000000l, 10);
      
      // populate
      for (int i = 0; i < count; i++) {
        Mutation m = new Mutation(new Text(String.format("%05d", i)));
        m.put(new Text("col" + Integer.toString((i % 3) + 1)), new Text("qual"), new Value("junk".getBytes()));
        b.addMutation(m);
      }
      
      b.close();
      
    }
    
    readBack(connector, count);
    
  }
}
