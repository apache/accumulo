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

import org.apache.accumulo.core.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ByteArraySet;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

public class ReadWriteExample {
  // defaults
  private static final String DEFAULT_AUTHS = "LEVEL1,GROUP1";
  private static final String DEFAULT_TABLE_NAME = "test";

  private Connector conn;

  static class Opts extends ClientOnDefaultTable {
    @Parameter(names = {"-C", "--createtable"}, description = "create table before doing anything")
    boolean createtable = false;
    @Parameter(names = {"-D", "--deletetable"}, description = "delete table when finished")
    boolean deletetable = false;
    @Parameter(names = {"-c", "--create"}, description = "create entries before any deletes")
    boolean createEntries = false;
    @Parameter(names = {"-r", "--read"}, description = "read entries after any creates/deletes")
    boolean readEntries = false;
    @Parameter(names = {"-d", "--delete"}, description = "delete entries after any creates")
    boolean deleteEntries = false;

    public Opts() {
      super(DEFAULT_TABLE_NAME);
      auths = new Authorizations(DEFAULT_AUTHS.split(","));
    }
  }

  // hidden constructor
  private ReadWriteExample() {}

  private void execute(Opts opts, ScannerOpts scanOpts) throws Exception {
    conn = opts.getConnector();

    // add the authorizations to the user
    Authorizations userAuthorizations = conn.securityOperations().getUserAuthorizations(opts.principal);
    ByteArraySet auths = new ByteArraySet(userAuthorizations.getAuthorizations());
    auths.addAll(opts.auths.getAuthorizations());
    if (!auths.isEmpty())
      conn.securityOperations().changeUserAuthorizations(opts.principal, new Authorizations(auths));

    // create table
    if (opts.createtable) {
      SortedSet<Text> partitionKeys = new TreeSet<Text>();
      for (int i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++)
        partitionKeys.add(new Text(new byte[] {(byte) i}));
      conn.tableOperations().create(opts.getTableName());
      conn.tableOperations().addSplits(opts.getTableName(), partitionKeys);
    }

    // send mutations
    createEntries(opts);

    // read entries
    if (opts.readEntries) {
      // Note that the user needs to have the authorizations for the specified scan authorizations
      // by an administrator first
      Scanner scanner = conn.createScanner(opts.getTableName(), opts.auths);
      scanner.setBatchSize(scanOpts.scanBatchSize);
      for (Entry<Key,Value> entry : scanner)
        System.out.println(entry.getKey().toString() + " -> " + entry.getValue().toString());
    }

    // delete table
    if (opts.deletetable)
      conn.tableOperations().delete(opts.getTableName());
  }

  private void createEntries(Opts opts) throws Exception {
    if (opts.createEntries || opts.deleteEntries) {
      BatchWriter writer = conn.createBatchWriter(opts.getTableName(), new BatchWriterConfig());
      ColumnVisibility cv = new ColumnVisibility(opts.auths.toString().replace(',', '|'));

      Text cf = new Text("datatypes");
      Text cq = new Text("xml");
      byte[] row = {'h', 'e', 'l', 'l', 'o', '\0'};
      byte[] value = {'w', 'o', 'r', 'l', 'd', '\0'};

      for (int i = 0; i < 10; i++) {
        row[row.length - 1] = (byte) i;
        Mutation m = new Mutation(new Text(row));
        if (opts.deleteEntries) {
          m.putDelete(cf, cq, cv);
        }
        if (opts.createEntries) {
          value[value.length - 1] = (byte) i;
          m.put(cf, cq, cv, new Value(value));
        }
        writer.addMutation(m);
      }
      writer.close();
    }
  }

  public static void main(String[] args) throws Exception {
    ReadWriteExample rwe = new ReadWriteExample();
    Opts opts = new Opts();
    ScannerOpts scanOpts = new ScannerOpts();
    opts.parseArgs(ReadWriteExample.class.getName(), args, scanOpts);
    rwe.execute(opts, scanOpts);
  }
}
