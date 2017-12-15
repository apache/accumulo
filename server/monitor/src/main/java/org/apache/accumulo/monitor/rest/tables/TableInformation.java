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
package org.apache.accumulo.monitor.rest.tables;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.master.thrift.TableInfo;

/**
 *
 * Generates table information as a JSON object
 *
 * @since 2.0.0
 *
 */
public class TableInformation {

  // Variable names become JSON keys
  public String tablename;
  public Table.ID tableId;
  public String tableState;

  public int tablets;
  public int onlineTablets;
  public long recs;
  public long recsInMemory;

  public double ingest;
  public double ingestByteRate;
  public double query;
  public double queryByteRate;

  public CompactionsList majorCompactions;
  public CompactionsList minorCompactions;
  public CompactionsList scans;

  private int queuedMajorCompactions;
  private int runningMajorCompactions;
  private int queuedMinorCompactions;
  private int runningMinorCompactions;
  private int queuedScans;
  private int runningScans;

  public double entriesRead;
  public double entriesReturned;

  public Double holdTime;

  public int offlineTablets;

  public TableInformation() {}

  /**
   * Generate a table with just the state
   *
   * @param tableName
   *          Table name to create
   * @param tableId
   *          Table ID to create
   * @param tableState
   *          State of the table
   */
  public TableInformation(String tableName, Table.ID tableId, String tableState) {
    this.tablename = tableName;
    this.tableId = tableId;
    this.tableState = tableState;
  }

  /**
   * Generate table based on the thrift table info
   *
   * @param tableName
   *          Name of the table to create
   * @param tableId
   *          ID of the table to create
   * @param info
   *          Thift table info
   * @param holdTime
   *          Hold time for the table
   * @param tableState
   *          State of the table
   */
  public TableInformation(String tableName, Table.ID tableId, TableInfo info, Double holdTime, String tableState) {
    this.tablename = tableName;
    this.tableId = tableId;

    this.tablets = info.tablets;
    this.offlineTablets = info.tablets - info.onlineTablets;
    this.onlineTablets = info.onlineTablets;

    this.recs = info.recs;
    this.recsInMemory = info.recsInMemory;

    this.ingest = info.getIngestRate();
    this.ingestByteRate = info.getIngestByteRate();

    this.query = info.getQueryRate();
    this.queryByteRate = info.getQueryByteRate();

    this.entriesRead = info.scanRate;
    this.entriesReturned = info.queryRate;

    this.holdTime = holdTime;

    if (null != info.scans) {
      this.queuedScans = info.scans.queued;
      this.runningScans = info.scans.running;
    } else {
      this.queuedScans = 0;
      this.runningScans = 0;
    }

    if (null != info.minors) {
      this.queuedMinorCompactions = info.minors.queued;
      this.runningMinorCompactions = info.minors.running;
    } else {
      this.queuedMinorCompactions = 0;
      this.runningMinorCompactions = 0;
    }

    if (null != info.majors) {
      this.queuedMajorCompactions = info.majors.queued;
      this.runningMajorCompactions = info.majors.running;
    } else {
      this.queuedMajorCompactions = 0;
      this.runningMajorCompactions = 0;
    }

    this.majorCompactions = new CompactionsList(runningMajorCompactions, queuedMajorCompactions);
    this.minorCompactions = new CompactionsList(runningMinorCompactions, queuedMinorCompactions);
    this.scans = new CompactionsList(runningScans, queuedScans);

    this.tableState = tableState;
  }
}
