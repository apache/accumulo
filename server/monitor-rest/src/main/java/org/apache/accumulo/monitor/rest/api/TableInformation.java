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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class TableInformation {
  protected String name, id;
  protected int tablets;
  protected int offlineTablets;
  protected long entries;
  protected long entriesInMemory;
  protected double ingest;
  protected double entriesRead, entriesReturned;
  protected double holdTime;
  protected int queuedScans, runningScans;
  protected int queuedMinorCompactions, runningMinorCompactions;
  protected int queuedMajorCompactions, runningMajorCompactions;
  protected String tableState;

  public TableInformation() {}

  public TableInformation(String tableName, String tableId, TableInfo info, double holdTime, String tableState) {
    this.name = tableName;
    this.id = tableId;

    this.tablets = info.tablets;
    this.offlineTablets = info.tablets - info.onlineTablets;

    this.entries = info.recs;
    this.entriesInMemory = info.recsInMemory;

    this.ingest = info.getIngestRate();

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

    this.tableState = tableState;
  }

  public TableInformation(TableInformation summary) {
    this.name = summary.getName();
    this.id = summary.getId();
    this.tablets = summary.getTablets();
    this.offlineTablets = summary.getOfflineTables();
    this.entries = summary.getEntries();
    this.entriesInMemory = summary.getEntriesInMemory();
    this.ingest = summary.getIngest();
    this.entriesRead = summary.getEntriesRead();
    this.entriesReturned = summary.getEntriesReturned();
    this.holdTime = summary.getHoldTime();
    this.queuedScans = summary.getQueuedScans();
    this.runningScans = summary.getRunningScans();
    this.queuedMinorCompactions = summary.getQueuedMinorCompactions();
    this.runningMinorCompactions = summary.getRunningMinorCompactions();
    this.queuedMajorCompactions = summary.getQueuedMajorCompactions();
    this.runningMajorCompactions = summary.getRunningMajorCompactions();
  }

  public TableInformation(String tableName, String tableId, int tablets, int offlineTablets, long entries, long entriesInMemory, double ingest,
      double entriesRead, double entriesReturned, long holdTime, int queuedScans, int runningScans, int queuedMinc, int runningMinc, int queuedMajc,
      int runningMajc) {
    this.name = tableName;
    this.id = tableId;
    this.tablets = tablets;
    this.offlineTablets = offlineTablets;
    this.entries = entries;
    this.entriesInMemory = entriesInMemory;
    this.ingest = ingest;
    this.entriesRead = entriesRead;
    this.entriesReturned = entriesReturned;
    this.holdTime = holdTime;
    this.queuedScans = queuedScans;
    this.runningScans = runningScans;
    this.queuedMinorCompactions = queuedMinc;
    this.runningMinorCompactions = runningMinc;
    this.queuedMajorCompactions = queuedMajc;
    this.runningMajorCompactions = runningMajc;
  }

  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("name")
  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty("id")
  public String getId() {
    return id;
  }

  @JsonProperty("id")
  public void setId(String id) {
    this.id = id;
  }

  @JsonProperty("tablets")
  public int getTablets() {
    return tablets;
  }

  @JsonProperty("tablets")
  public void setTablets(int tablets) {
    this.tablets = tablets;
  }

  @JsonProperty("offlineTablets")
  public int getOfflineTables() {
    return offlineTablets;
  }

  @JsonProperty("offlineTablets")
  public void setOfflineTables(int offlineTables) {
    this.offlineTablets = offlineTables;
  }

  @JsonProperty("entries")
  public long getEntries() {
    return entries;
  }

  @JsonProperty("entries")
  public void setEntries(long entries) {
    this.entries = entries;
  }

  @JsonProperty("entriesInMemory")
  public long getEntriesInMemory() {
    return entriesInMemory;
  }

  @JsonProperty("entriesInMemory")
  public void setEntriesInMemory(long entriesInMemory) {
    this.entriesInMemory = entriesInMemory;
  }

  @JsonProperty("ingest")
  public double getIngest() {
    return ingest;
  }

  @JsonProperty("ingest")
  public void setIngest(double ingest) {
    this.ingest = ingest;
  }

  @JsonProperty("entriesRead")
  public double getEntriesRead() {
    return entriesRead;
  }

  @JsonProperty("entriesRead")
  public void setEntriesRead(double entriesRead) {
    this.entriesRead = entriesRead;
  }

  @JsonProperty("entriesReturned")
  public double getEntriesReturned() {
    return entriesReturned;
  }

  @JsonProperty("entriesReturned")
  public void setEntriesReturned(double entriesReturned) {
    this.entriesReturned = entriesReturned;
  }

  @JsonProperty("holdTime")
  public double getHoldTime() {
    return holdTime;
  }

  @JsonProperty("holdTime")
  public void setHoldTime(long holdTime) {
    this.holdTime = holdTime;
  }

  @JsonProperty("runningScans")
  public int getRunningScans() {
    return runningScans;
  }

  @JsonProperty("runningScans")
  public void setRunningScans(int runningScans) {
    this.runningScans = runningScans;
  }

  @JsonProperty("queuedScans")
  public int getQueuedScans() {
    return queuedScans;
  }

  @JsonProperty("queuedScans")
  public void setQueuedScans(int queuedScans) {
    this.queuedScans = queuedScans;
  }

  @JsonProperty("runningMinorCompactions")
  public int getRunningMinorCompactions() {
    return runningMinorCompactions;
  }

  @JsonProperty("runningMinorCompactions")
  public void setRunningMinorCompactions(int runningMinorCompactions) {
    this.runningMinorCompactions = runningMinorCompactions;
  }

  @JsonProperty("queuedMinorCompactions")
  public int getQueuedMinorCompactions() {
    return queuedMinorCompactions;
  }

  @JsonProperty("queuedMinorCompactions")
  public void setQueuedMinorCompactions(int queuedMinorCompactions) {
    this.queuedMinorCompactions = queuedMinorCompactions;
  }

  @JsonProperty("runningMajorCompactions")
  public int getRunningMajorCompactions() {
    return runningMajorCompactions;
  }

  @JsonProperty("runningMajorCompactions")
  public void setRunningMajorCompactions(int runningMajorCompactions) {
    this.runningMajorCompactions = runningMajorCompactions;
  }

  @JsonProperty("queuedMajorCompactions")
  public int getQueuedMajorCompactions() {
    return queuedMajorCompactions;
  }

  @JsonProperty("queuedMajorCompactions")
  public void setQueuedMajorCompactions(int queuedMajc) {
    this.queuedMajorCompactions = queuedMajc;
  }

  @JsonProperty("offlineTablets")
  public int getOfflineTablets() {
    return offlineTablets;
  }

  @JsonProperty("offlineTablets")
  public void setOfflineTablets(int offlineTablets) {
    this.offlineTablets = offlineTablets;
  }

}
