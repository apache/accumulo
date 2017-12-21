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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

public class QueryMetadataTable {
  private static final Logger log = LoggerFactory.getLogger(QueryMetadataTable.class);

  private static String principal;
  private static AuthenticationToken token;

  static String location;

  static class MDTQuery implements Runnable {
    private Text row;

    MDTQuery(Text row) {
      this.row = row;
    }

    @Override
    public void run() {
      Scanner mdScanner = null;
      try {
        KeyExtent extent = new KeyExtent(row, (Text) null);

        Connector connector = HdfsZooInstance.getInstance().getConnector(principal, token);
        mdScanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
        Text row = extent.getMetadataEntry();

        mdScanner.setRange(new Range(row));

        for (Entry<Key,Value> entry : mdScanner) {
          if (!entry.getKey().getRow().equals(row))
            break;
        }

      } catch (TableNotFoundException e) {
        log.error("Table '" + MetadataTable.NAME + "' not found.", e);
        throw new RuntimeException(e);
      } catch (AccumuloException e) {
        log.error("AccumuloException encountered.", e);
        throw new RuntimeException(e);
      } catch (AccumuloSecurityException e) {
        log.error("AccumuloSecurityException encountered.", e);
        throw new RuntimeException(e);
      } finally {
        if (mdScanner != null) {
          mdScanner.close();
        }
      }
    }
  }

  static class Opts extends ClientOpts {
    @Parameter(names = "--numQueries", description = "number of queries to run")
    int numQueries = 1;
    @Parameter(names = "--numThreads", description = "number of threads used to run the queries")
    int numThreads = 1;
  }

  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    Opts opts = new Opts();
    ScannerOpts scanOpts = new ScannerOpts();
    opts.parseArgs(QueryMetadataTable.class.getName(), args, scanOpts);

    Connector connector = opts.getConnector();
    HashSet<Text> rowSet = new HashSet<>();

    int count = 0;

    try (Scanner scanner = connector.createScanner(MetadataTable.NAME, opts.auths)) {
      scanner.setBatchSize(scanOpts.scanBatchSize);
      Text mdrow = new Text(KeyExtent.getMetadataEntry(MetadataTable.ID, null));

      for (Entry<Key,Value> entry : scanner) {
        System.out.print(".");
        if (count % 72 == 0) {
          System.out.printf(" %,d%n", count);
        }
        if (entry.getKey().compareRow(mdrow) == 0 && entry.getKey().getColumnFamily().compareTo(TabletsSection.CurrentLocationColumnFamily.NAME) == 0) {
          System.out.println(entry.getKey() + " " + entry.getValue());
          location = entry.getValue().toString();
        }

        if (!entry.getKey().getRow().toString().startsWith(MetadataTable.ID.canonicalID()))
          rowSet.add(entry.getKey().getRow());
        count++;
      }
    }

    System.out.printf(" %,d%n", count);

    ArrayList<Text> rows = new ArrayList<>(rowSet);

    Random r = new Random();

    ExecutorService tp = Executors.newFixedThreadPool(opts.numThreads);

    long t1 = System.currentTimeMillis();

    for (int i = 0; i < opts.numQueries; i++) {
      int index = r.nextInt(rows.size());
      MDTQuery mdtq = new MDTQuery(rows.get(index));
      tp.submit(mdtq);
    }

    tp.shutdown();

    try {
      tp.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      log.error("Failed while awaiting the ExcecutorService to terminate.", e);
      throw new RuntimeException(e);
    }

    long t2 = System.currentTimeMillis();
    double delta = (t2 - t1) / 1000.0;
    System.out.println("time : " + delta + "  queries per sec : " + (opts.numQueries / delta));
  }
}
