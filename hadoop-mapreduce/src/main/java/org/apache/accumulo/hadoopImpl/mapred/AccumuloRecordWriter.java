/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.hadoopImpl.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.hadoop.mapred.AccumuloOutputFormat;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.OutputConfigurator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base class to be used to create {@link RecordWriter} instances that write to Accumulo.
 */
public class AccumuloRecordWriter implements RecordWriter<Text,Mutation> {
  // class to serialize configuration under in the job
  private static final Class<AccumuloOutputFormat> CLASS = AccumuloOutputFormat.class;
  private static final Logger log = LoggerFactory.getLogger(AccumuloRecordWriter.class);
  private MultiTableBatchWriter mtbw = null;
  private HashMap<Text,BatchWriter> bws;
  private Text defaultTableName;

  private boolean simulate;
  private boolean createTables;

  private long mutCount = 0;
  private long valCount = 0;

  private AccumuloClient client;

  public AccumuloRecordWriter(JobConf job) {
    this.simulate = OutputConfigurator.getSimulationMode(CLASS, job);
    this.createTables = OutputConfigurator.canCreateTables(CLASS, job);

    if (simulate) {
      log.info("Simulating output only. No writes to tables will occur");
    }

    this.bws = new HashMap<>();

    String tname = OutputConfigurator.getDefaultTableName(CLASS, job);
    this.defaultTableName = (tname == null) ? null : new Text(tname);

    if (!simulate) {
      this.client = OutputConfigurator.createClient(CLASS, job);
      mtbw = client.createMultiTableBatchWriter();
    }
  }

  /**
   * Push a mutation into a table. If table is null, the defaultTable will be used. If
   * OutputFormatBuilder#createTables() is set, the table will be created if it does not exist. The
   * table name must only contain alphanumerics and underscore.
   */
  @Override
  public void write(Text table, Mutation mutation) throws IOException {
    if (table == null || table.toString().isEmpty()) {
      table = this.defaultTableName;
    }

    if (!simulate && table == null) {
      throw new IOException("No table or default table specified. Try simulation mode next time");
    }

    ++mutCount;
    valCount += mutation.size();
    printMutation(table, mutation);

    if (simulate) {
      return;
    }

    if (!bws.containsKey(table)) {
      try {
        addTable(table);
      } catch (final AccumuloSecurityException | AccumuloException e) {
        log.error("Could not add table '" + table + "'", e);
        throw new IOException(e);
      }
    }

    try {
      bws.get(table).addMutation(mutation);
    } catch (MutationsRejectedException e) {
      throw new IOException(e);
    }
  }

  protected void addTable(Text tableName) throws AccumuloException, AccumuloSecurityException {
    if (simulate) {
      log.info("Simulating adding table: " + tableName);
      return;
    }

    log.debug("Adding table: " + tableName);
    BatchWriter bw = null;
    String table = tableName.toString();

    if (createTables && !client.tableOperations().exists(table)) {
      try {
        client.tableOperations().create(table);
      } catch (AccumuloSecurityException e) {
        log.error("Accumulo security violation creating " + table, e);
        throw e;
      } catch (TableExistsException e) {
        // Shouldn't happen
      }
    }

    try {
      bw = mtbw.getBatchWriter(table);
    } catch (TableNotFoundException e) {
      log.error("Accumulo table " + table + " doesn't exist and cannot be created.", e);
      throw new AccumuloException(e);
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw e;
    }

    if (bw != null) {
      bws.put(tableName, bw);
    }
  }

  private int printMutation(Text table, Mutation m) {
    if (log.isTraceEnabled()) {
      log.trace(String.format("Table %s row key: %s", table, hexDump(m.getRow())));
      for (ColumnUpdate cu : m.getUpdates()) {
        log.trace(String.format("Table %s column: %s:%s", table, hexDump(cu.getColumnFamily()),
            hexDump(cu.getColumnQualifier())));
        log.trace(String.format("Table %s security: %s", table,
            new ColumnVisibility(cu.getColumnVisibility()).toString()));
        log.trace(String.format("Table %s value: %s", table, hexDump(cu.getValue())));
      }
    }
    return m.getUpdates().size();
  }

  private String hexDump(byte[] ba) {
    StringBuilder sb = new StringBuilder();
    for (byte b : ba) {
      if ((b > 0x20) && (b < 0x7e)) {
        sb.append((char) b);
      } else {
        sb.append(String.format("x%02x", b));
      }
    }
    return sb.toString();
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    log.debug("mutations written: " + mutCount + ", values written: " + valCount);
    if (simulate) {
      return;
    }

    try {
      mtbw.close();
    } catch (MutationsRejectedException e) {
      if (!e.getSecurityErrorCodes().isEmpty()) {
        var tables = new HashMap<String,Set<SecurityErrorCode>>();
        e.getSecurityErrorCodes().forEach((tabletId, codes) -> tables
            .computeIfAbsent(tabletId.getTable().canonical(), k -> new HashSet<>()).addAll(codes));
        log.error("Not authorized to write to tables : " + tables);
      }

      if (!e.getConstraintViolationSummaries().isEmpty()) {
        log.error("Constraint violations : " + e.getConstraintViolationSummaries().size());
      }
      throw new IOException(e);
    } finally {
      client.close();
    }
  }
}
