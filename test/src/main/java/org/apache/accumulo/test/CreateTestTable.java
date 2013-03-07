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
package org.apache.accumulo.test;

import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.accumulo.server.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

public class CreateTestTable {
  
  static class Opts extends ClientOnDefaultTable {
    @Parameter(names={"-readonly", "--readonly"}, description="read only")
    boolean readOnly = false;
    @Parameter(names={"-count", "--count"}, description="count", required = true)
    int count = 10000;
    Opts() { super("mrtest1"); }
  }
  
  private static void readBack(Connector conn, Opts opts, ScannerOpts scanOpts) throws Exception {
    Scanner scanner = conn.createScanner("mrtest1", opts.auths);
    scanner.setBatchSize(scanOpts.scanBatchSize);
    int count = 0;
    for (Entry<Key,Value> elt : scanner) {
      String expected = String.format("%05d", count);
      assert (elt.getKey().getRow().toString().equals(expected));
      count++;
    }
    assert (opts.count == count);
  }
  
  public static void main(String[] args) throws Exception {
    String program = CreateTestTable.class.getName();
    Opts opts = new Opts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    ScannerOpts scanOpts = new ScannerOpts();
    opts.parseArgs(program, args, bwOpts, scanOpts);
    
    // create the test table within accumulo
    Connector connector = opts.getConnector();
    
    if (!opts.readOnly) {
      TreeSet<Text> keys = new TreeSet<Text>();
      for (int i = 0; i < opts.count / 100; i++) {
        keys.add(new Text(String.format("%05d", i * 100)));
      }
      
      // presplit
      connector.tableOperations().create(opts.getTableName());
      connector.tableOperations().addSplits(opts.getTableName(), keys);
      BatchWriter b = connector.createBatchWriter(opts.getTableName(), bwOpts.getBatchWriterConfig());
      
      // populate
      for (int i = 0; i < opts.count; i++) {
        Mutation m = new Mutation(new Text(String.format("%05d", i)));
        m.put(new Text("col" + Integer.toString((i % 3) + 1)), new Text("qual"), new Value("junk".getBytes()));
        b.addMutation(m);
      }
      b.close();
    }
    
    readBack(connector, opts, scanOpts);
    opts.stopTracing();
  }
}
