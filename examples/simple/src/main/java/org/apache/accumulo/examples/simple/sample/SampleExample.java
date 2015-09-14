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

package org.apache.accumulo.examples.simple.sample;

import java.util.Collections;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;
import org.apache.accumulo.core.client.admin.SamplerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.sample.RowSampler;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.examples.simple.client.RandomBatchWriter;
import org.apache.accumulo.examples.simple.shard.CutoffIntersectingIterator;

import com.google.common.collect.ImmutableMap;

/**
 * A simple example of using Accumulo's sampling feature. This example does something similar to what README.sample shows using the shell. Also see
 * {@link CutoffIntersectingIterator} and README.sample for an example of how to use sample data from within an iterator.
 */
public class SampleExample {

  // a compaction strategy that only selects files for compaction that have no sample data or sample data created in a different way than the tables
  static final CompactionStrategyConfig NO_SAMPLE_STRATEGY = new CompactionStrategyConfig(
      "org.apache.accumulo.tserver.compaction.strategies.ConfigurableCompactionStrategy").setOptions(Collections.singletonMap("SF_NO_SAMPLE", ""));

  static class Opts extends ClientOnDefaultTable {
    public Opts() {
      super("sampex");
    }
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.parseArgs(RandomBatchWriter.class.getName(), args, bwOpts);

    Connector conn = opts.getConnector();

    if (!conn.tableOperations().exists(opts.getTableName())) {
      conn.tableOperations().create(opts.getTableName());
    } else {
      System.out.println("Table exists, not doing anything.");
      return;
    }

    // write some data
    BatchWriter bw = conn.createBatchWriter(opts.getTableName(), bwOpts.getBatchWriterConfig());
    bw.addMutation(createMutation("9225", "abcde", "file://foo.txt"));
    bw.addMutation(createMutation("8934", "accumulo scales", "file://accumulo_notes.txt"));
    bw.addMutation(createMutation("2317", "milk, eggs, bread, parmigiano-reggiano", "file://groceries/9/txt"));
    bw.addMutation(createMutation("3900", "EC2 ate my homework", "file://final_project.txt"));
    bw.flush();

    SamplerConfiguration sc1 = new SamplerConfiguration(RowSampler.class.getName());
    sc1.setOptions(ImmutableMap.of("hasher", "murmur3_32", "modulus", "3"));

    conn.tableOperations().setSamplerConfiguration(opts.getTableName(), sc1);

    Scanner scanner = conn.createScanner(opts.getTableName(), Authorizations.EMPTY);
    System.out.println("Scanning all data :");
    print(scanner);
    System.out.println();

    System.out.println("Scanning with sampler configuration.  Data was written before sampler was set on table, scan should fail.");
    scanner.setSamplerConfiguration(sc1);
    try {
      print(scanner);
    } catch (SampleNotPresentException e) {
      System.out.println("  Saw sample not present exception as expected.");
    }
    System.out.println();

    // compact table to recreate sample data
    conn.tableOperations().compact(opts.getTableName(), new CompactionConfig().setCompactionStrategy(NO_SAMPLE_STRATEGY));

    System.out.println("Scanning after compaction (compaction should have created sample data) : ");
    print(scanner);
    System.out.println();

    // update a document in the sample data
    bw.addMutation(createMutation("2317", "milk, eggs, bread, parmigiano-reggiano, butter", "file://groceries/9/txt"));
    bw.close();
    System.out.println("Scanning sample after updating content for docId 2317 (should see content change in sample data) : ");
    print(scanner);
    System.out.println();

    // change tables sampling configuration...
    SamplerConfiguration sc2 = new SamplerConfiguration(RowSampler.class.getName());
    sc2.setOptions(ImmutableMap.of("hasher", "murmur3_32", "modulus", "2"));
    conn.tableOperations().setSamplerConfiguration(opts.getTableName(), sc2);
    // compact table to recreate sample data using new configuration
    conn.tableOperations().compact(opts.getTableName(), new CompactionConfig().setCompactionStrategy(NO_SAMPLE_STRATEGY));

    System.out.println("Scanning with old sampler configuration.  Sample data was created using new configuration with a compaction.  Scan should fail.");
    try {
      // try scanning with old sampler configuration
      print(scanner);
    } catch (SampleNotPresentException e) {
      System.out.println("  Saw sample not present exception as expected ");
    }
    System.out.println();

    // update expected sampler configuration on scanner
    scanner.setSamplerConfiguration(sc2);

    System.out.println("Scanning with new sampler configuration : ");
    print(scanner);
    System.out.println();

  }

  private static void print(Scanner scanner) {
    for (Entry<Key,Value> entry : scanner) {
      System.out.println("  " + entry.getKey() + " " + entry.getValue());
    }
  }

  private static Mutation createMutation(String docId, String content, String url) {
    Mutation m = new Mutation(docId);
    m.put("doc", "context", content);
    m.put("doc", "url", url);
    return m;
  }
}
