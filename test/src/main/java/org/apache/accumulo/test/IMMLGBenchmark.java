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

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.core.util.Stat;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Iterators;

/**
 *
 */
public class IMMLGBenchmark {
  public static void main(String[] args) throws Exception {
    ZooKeeperInstance zki = new ZooKeeperInstance(new ClientConfiguration().withInstance("test16").withZkHosts("localhost"));
    Connector conn = zki.getConnector("root", new PasswordToken("secret"));

    int numlg = Integer.parseInt(args[0]);

    ArrayList<byte[]> cfset = new ArrayList<>();

    for (int i = 0; i < 32; i++) {
      cfset.add(String.format("%04x", i).getBytes());
    }

    Map<String,Stat> stats = new TreeMap<>();

    for (int i = 0; i < 5; i++) {
      runTest(conn, numlg, cfset, i > 1 ? stats : null);
      System.out.println();
    }

    for (Entry<String,Stat> entry : stats.entrySet()) {
      System.out.printf("%20s : %6.2f\n", entry.getKey(), entry.getValue().getAverage());
    }

  }

  private static void runTest(Connector conn, int numlg, ArrayList<byte[]> cfset, Map<String,Stat> stats) throws Exception {
    String table = "immlgb";

    try {
      conn.tableOperations().delete(table);
    } catch (TableNotFoundException tnfe) {}

    conn.tableOperations().create(table);
    conn.tableOperations().setProperty(table, Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "snappy");

    setupLocalityGroups(conn, numlg, cfset, table);

    addStat(stats, "write", write(conn, cfset, table));
    addStat(stats, "scan cf", scan(conn, cfset, table, false));
    addStat(stats, "scan cf:cq", scan(conn, cfset, table, true));
    // TODO time reading all data

    long t1 = System.currentTimeMillis();
    conn.tableOperations().flush(table, null, null, true);
    long t2 = System.currentTimeMillis();

    addStat(stats, "flush", t2 - t1);
  }

  private static void addStat(Map<String,Stat> stats, String s, long wt) {
    System.out.println(s + ":" + wt);

    if (stats == null)
      return;

    Stat stat = stats.get(s);
    if (stat == null) {
      stat = new Stat();
      stats.put(s, stat);
    }
    stat.addStat(wt);
  }

  private static long scan(Connector conn, ArrayList<byte[]> cfset, String table, boolean cq) throws TableNotFoundException {
    Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);

    if (!cq)
      scanner.fetchColumnFamily(new Text(cfset.get(15)));
    else
      scanner.fetchColumn(new Text(cfset.get(15)), new Text(cfset.get(15)));

    long t1 = System.currentTimeMillis();

    Iterators.size(scanner.iterator());

    long t2 = System.currentTimeMillis();

    return t2 - t1;

  }

  private static long write(Connector conn, ArrayList<byte[]> cfset, String table) throws TableNotFoundException, MutationsRejectedException {
    Random rand = new Random();

    byte val[] = new byte[50];

    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());

    long t1 = System.currentTimeMillis();

    for (int i = 0; i < 1 << 15; i++) {
      byte[] row = FastFormat.toZeroPaddedString(abs(rand.nextLong()), 16, 16, new byte[0]);

      Mutation m = new Mutation(row);
      for (byte[] cf : cfset) {
        byte[] cq = FastFormat.toZeroPaddedString(rand.nextInt(1 << 16), 4, 16, new byte[0]);
        rand.nextBytes(val);
        m.put(cf, cq, val);
      }

      bw.addMutation(m);
    }

    bw.close();

    long t2 = System.currentTimeMillis();

    return t2 - t1;
  }

  private static void setupLocalityGroups(Connector conn, int numlg, ArrayList<byte[]> cfset, String table) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException {
    if (numlg > 1) {
      int numCF = cfset.size() / numlg;
      int gNum = 0;

      Iterator<byte[]> cfiter = cfset.iterator();
      Map<String,Set<Text>> groups = new HashMap<>();
      while (cfiter.hasNext()) {
        HashSet<Text> groupCols = new HashSet<>();
        for (int i = 0; i < numCF && cfiter.hasNext(); i++) {
          groupCols.add(new Text(cfiter.next()));
        }

        groups.put("lg" + (gNum++), groupCols);
      }

      conn.tableOperations().setLocalityGroups(table, groups);
      conn.tableOperations().offline(table);
      sleepUninterruptibly(1, TimeUnit.SECONDS);
      conn.tableOperations().online(table);
    }
  }

  public static long abs(long l) {
    l = Math.abs(l); // abs(Long.MIN_VALUE) == Long.MIN_VALUE...
    if (l < 0)
      return 0;
    return l;
  }
}
