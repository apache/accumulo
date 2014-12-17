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

import static com.google.common.base.Charsets.UTF_8;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

public class TestMultiTableIngest {

  static class Opts extends ClientOpts {
    @Parameter(names = "--readonly", description = "read only")
    boolean readonly = false;
    @Parameter(names = "--tables", description = "number of tables to create")
    int tables = 5;
    @Parameter(names = "--count", description = "number of entries to create")
    int count = 10000;
    @Parameter(names = "--tablePrefix", description = "prefix of the table names")
    String prefix = "test_";
  }

  private static void readBack(Opts opts, ScannerOpts scanOpts, Connector conn, List<String> tableNames) throws Exception {
    int i = 0;
    for (String table : tableNames) {
      // wait for table to exist
      while (!conn.tableOperations().exists(table))
        UtilWaitThread.sleep(100);
      Scanner scanner = conn.createScanner(table, opts.auths);
      scanner.setBatchSize(scanOpts.scanBatchSize);
      int count = i;
      for (Entry<Key,Value> elt : scanner) {
        String expected = String.format("%06d", count);
        if (!elt.getKey().getRow().toString().equals(expected))
          throw new RuntimeException("entry " + elt + " does not match expected " + expected + " in table " + table);
        count += tableNames.size();
      }
      i++;
    }
  }

  public static void main(String[] args) throws Exception {
    ArrayList<String> tableNames = new ArrayList<String>();

    Opts opts = new Opts();
    ScannerOpts scanOpts = new ScannerOpts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.parseArgs(TestMultiTableIngest.class.getName(), args, scanOpts, bwOpts);
    // create the test table within accumulo
    Connector connector;
    try {
      connector = opts.getConnector();
    } catch (AccumuloException e) {
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      throw new RuntimeException(e);
    }
    for (int i = 0; i < opts.tables; i++) {
      tableNames.add(String.format(opts.prefix + "%04d", i));
    }

    if (!opts.readonly) {
      for (String table : tableNames)
        connector.tableOperations().create(table);

      MultiTableBatchWriter b;
      try {
        b = connector.createMultiTableBatchWriter(bwOpts.getBatchWriterConfig());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      // populate
      for (int i = 0; i < opts.count; i++) {
        Mutation m = new Mutation(new Text(String.format("%06d", i)));
        m.put(new Text("col" + Integer.toString((i % 3) + 1)), new Text("qual"), new Value("junk".getBytes(UTF_8)));
        b.getBatchWriter(tableNames.get(i % tableNames.size())).addMutation(m);
      }
      try {
        b.close();
      } catch (MutationsRejectedException e) {
        throw new RuntimeException(e);
      }
    }
    try {
      readBack(opts, scanOpts, connector, tableNames);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
