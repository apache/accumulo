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
package org.apache.accumulo.test.scalability;

import static com.google.common.base.Charsets.UTF_8;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.test.continuous.ContinuousIngest;

public class Ingest extends ScaleTest {

  @Override
  public void setup() {

    Connector conn = getConnector();
    String tableName = getTestProperty("TABLE");

    // delete existing table
    if (conn.tableOperations().exists(tableName)) {
      System.out.println("Deleting existing table: " + tableName);
      try {
        conn.tableOperations().delete(tableName);
      } catch (Exception e) {
        System.out.println("Failed to delete table: " + tableName);
        e.printStackTrace();
      }
    }

    // create table
    try {
      conn.tableOperations().create(tableName);
      conn.tableOperations().addSplits(tableName, calculateSplits());
      conn.tableOperations().setProperty(tableName, "table.split.threshold", "256M");
    } catch (Exception e) {
      System.out.println("Failed to create table: " + tableName);
      e.printStackTrace();
    }

  }

  @Override
  public void client() {

    Connector conn = getConnector();
    String tableName = getTestProperty("TABLE");

    // get batch writer configuration
    long maxMemory = Long.parseLong(getTestProperty("MAX_MEMORY"));
    long maxLatency = Long.parseLong(getTestProperty("MAX_LATENCY"));
    int maxWriteThreads = Integer.parseInt(getTestProperty("NUM_THREADS"));

    // create batch writer
    BatchWriter bw = null;
    try {
      bw = conn.createBatchWriter(tableName, new BatchWriterConfig().setMaxMemory(maxMemory).setMaxLatency(maxLatency, TimeUnit.MILLISECONDS)
          .setMaxWriteThreads(maxWriteThreads));
    } catch (TableNotFoundException e) {
      System.out.println("Table not found: " + tableName);
      e.printStackTrace();
    }

    // configure writing
    Random r = new Random();
    String ingestInstanceId = UUID.randomUUID().toString();
    long numIngestEntries = Long.parseLong(getTestProperty("NUM_ENTRIES"));
    long minRow = 0L;
    long maxRow = 9223372036854775807L;
    int maxColF = 32767;
    int maxColQ = 32767;
    long count = 0;
    long totalBytes = 0;

    ColumnVisibility cv = new ColumnVisibility();

    // start timer
    startTimer();

    // write specified number of entries
    while (count < numIngestEntries) {
      count++;
      long rowId = ContinuousIngest.genLong(minRow, maxRow, r);
      Mutation m = ContinuousIngest.genMutation(rowId, r.nextInt(maxColF), r.nextInt(maxColQ), cv, ingestInstanceId.getBytes(UTF_8), count, null, r, false);
      totalBytes += m.numBytes();
      try {
        bw.addMutation(m);
      } catch (MutationsRejectedException e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }

    // close writer
    try {
      bw.close();
    } catch (MutationsRejectedException e) {
      e.printStackTrace();
      System.exit(-1);
    }

    // stop timer
    stopTimer(count, totalBytes);
  }

  @Override
  public void teardown() {

    Connector conn = getConnector();
    String tableName = getTestProperty("TABLE");

    try {
      conn.tableOperations().delete(tableName);
    } catch (Exception e) {
      System.out.println("Failed to delete table: " + tableName);
      e.printStackTrace();
    }
  }

}
