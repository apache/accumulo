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
package org.apache.accumulo.monitor.rest.tservers;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;

import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.tables.CompactionsList;
import org.apache.accumulo.monitor.rest.tables.CompactionsTypes;
import org.apache.accumulo.monitor.rest.trace.RecoveryStatusInformation;
import org.apache.accumulo.server.util.TableInfoUtil;

/**
 *
 * Generates tserver information
 *
 * @since 2.0.0
 *
 */
public class TabletServerInformation {

  // Variable names become JSON keys
  @XmlAttribute(name = "id")
  public String server;

  public String hostname;
  public long lastContact;
  public long responseTime;
  public double osload;
  public String version;

  public CompactionsTypes compactions;

  public int tablets;
  public double ingest;
  public double query;
  public double ingestMB;
  public double queryMB;
  public Integer scans; // For backwards compatibility, has same information as scansRunning
  public Double scansessions;
  public Double scanssessions; // For backwards compatibility
  public long holdtime;

  // New variables

  public String ip;
  private Integer scansRunning;
  private Integer scansQueued;
  private Integer minorRunning;
  private Integer minorQueued;
  private Integer majorRunning;
  private Integer majorQueued;
  private CompactionsList scansCompacting; // if scans is removed, change scansCompacting to scans
  private CompactionsList major;
  private CompactionsList minor;
  public long entries;
  public long lookups;
  public long indexCacheHits;
  public long indexCacheRequests;
  public long dataCacheHits;
  public long dataCacheRequests;
  public double indexCacheHitRate;
  public double dataCacheHitRate;
  public List<RecoveryStatusInformation> logRecoveries;

  public TabletServerInformation() {}

  /**
   * Generate tserver information from thrift status
   *
   * @param thriftStatus
   *          Thrift status to obtain information
   */
  public TabletServerInformation(TabletServerStatus thriftStatus) {
    TableInfo summary = TableInfoUtil.summarizeTableStats(thriftStatus);
    updateTabletServerInfo(thriftStatus, summary);
  }

  /**
   * Generate tserver information from thrift status and table summary
   *
   * @param thriftStatus
   *          Thrift status to obtain information
   * @param summary
   *          Table info summary
   */
  public void updateTabletServerInfo(TabletServerStatus thriftStatus, TableInfo summary) {

    long now = System.currentTimeMillis();

    this.server = this.ip = this.hostname = thriftStatus.name;
    this.tablets = summary.tablets;
    this.lastContact = now - thriftStatus.lastContact;
    this.responseTime = thriftStatus.responseTime;
    this.entries = summary.recs;
    this.ingest = summary.ingestRate;
    this.query = summary.queryRate;

    this.holdtime = thriftStatus.holdTime;

    this.scansRunning = summary.scans != null ? summary.scans.running : null;
    this.scansQueued = summary.scans != null ? summary.scans.queued : null;

    this.scans = this.scansRunning;

    this.scansCompacting = new CompactionsList(this.scansRunning, this.scansQueued);

    this.minorRunning = summary.minors != null ? summary.minors.running : null;
    this.minorQueued = summary.minors != null ? summary.minors.queued : null;

    this.minor = new CompactionsList(this.minorRunning, this.minorQueued);

    this.majorRunning = summary.majors != null ? summary.majors.running : null;
    this.majorQueued = summary.majors != null ? summary.majors.queued : null;

    this.major = new CompactionsList(this.majorRunning, this.majorQueued);

    this.compactions = new CompactionsTypes(scansCompacting, major, minor);

    this.osload = thriftStatus.osLoad;
    this.version = thriftStatus.version;
    this.lookups = thriftStatus.lookups;

    this.dataCacheHits = thriftStatus.dataCacheHits;
    this.dataCacheRequests = thriftStatus.dataCacheRequest;
    this.indexCacheHits = thriftStatus.indexCacheHits;
    this.indexCacheRequests = thriftStatus.indexCacheRequest;

    this.indexCacheHitRate = this.indexCacheHits / (double) Math.max(this.indexCacheRequests, 1);
    this.dataCacheHitRate = this.dataCacheHits / (double) Math.max(this.dataCacheRequests, 1);

    this.ingestMB = summary.ingestByteRate;
    this.queryMB = summary.queryByteRate;

    this.scansessions = Monitor.getLookupRate();
    this.scanssessions = this.scansessions; // For backwards compatibility

    this.logRecoveries = new ArrayList<>(thriftStatus.logSorts.size());
    for (RecoveryStatus recovery : thriftStatus.logSorts) {
      logRecoveries.add(new RecoveryStatusInformation(recovery));
    }
  }
}
