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
package org.apache.accumulo.monitor.rest.api;

import org.apache.accumulo.core.master.thrift.TableInfo;

public class TableInformation {

  public String tablename, tableId, tableState;

  public int tablets, onlineTablets;
  public long recs, recsInMemory;

  public double ingest, ingestByteRate, query, queryByteRate;

  public CompactionsList majorCompactions, minorCompactions, scans;

  private int queuedMajorCompactions, runningMajorCompactions, queuedMinorCompactions, runningMinorCompactions, queuedScans, runningScans;

  public double entriesRead, entriesReturned;
  public Double holdTime;

  public int offlineTablets;

  public TableInformation() {}

  public TableInformation(String tableName, String tableId, String tableState) {
    this.tablename = tableName;
    this.tableId = tableId;
    this.tableState = tableState;
  }

  public TableInformation(String tableName, String tableId, TableInfo info, Double holdTime, String tableState) {
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
