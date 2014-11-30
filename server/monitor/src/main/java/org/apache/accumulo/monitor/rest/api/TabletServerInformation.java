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

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class TabletServerInformation {
  private String address;
  private long lastContact, holdTime, lookups, indexCacheHits, indexCacheRequests, dataCacheHits, dataCacheRequests;
  private double osLoad;
  private List<RecoveryStatusInformation> logRecoveries;

  public TabletServerInformation() {}

  public TabletServerInformation(String address, double osLoad, long holdTime, long lookups, long indexCacheHits, long indexCacheRequests, long dataCacheHits,
      long dataCacheRequests, List<RecoveryStatusInformation> logRecoveries) {
    this.address = address;
    this.osLoad = osLoad;
    this.holdTime = holdTime;
    this.lookups = lookups;
    this.indexCacheHits = indexCacheHits;
    this.indexCacheRequests = indexCacheRequests;
    this.dataCacheHits = dataCacheHits;
    this.dataCacheRequests = dataCacheRequests;
    this.logRecoveries = logRecoveries;
  }

  public TabletServerInformation(TabletServerStatus thriftStatus) {
    this.address = thriftStatus.name;
    this.osLoad = thriftStatus.osLoad;
    this.holdTime = thriftStatus.holdTime;
    this.lookups = thriftStatus.lookups;

    this.dataCacheHits = thriftStatus.dataCacheHits;
    this.dataCacheRequests = thriftStatus.dataCacheRequest;
    this.indexCacheHits = thriftStatus.indexCacheHits;
    this.indexCacheRequests = thriftStatus.indexCacheRequest;

    this.logRecoveries = new ArrayList<>(thriftStatus.logSorts.size());
    for (RecoveryStatus recovery : thriftStatus.logSorts) {
      logRecoveries.add(new RecoveryStatusInformation(recovery));
    }
  }

  @JsonProperty("address")
  public String getAddress() {
    return address;
  }

  @JsonProperty("address")
  public void setAddress(String address) {
    this.address = address;
  }

  @JsonProperty("lastContact")
  public long getLastContact() {
    return lastContact;
  }

  @JsonProperty("lastContact")
  public void setLastContact(long lastContact) {
    this.lastContact = lastContact;
  }

  @JsonProperty("holdTime")
  public long getHoldTime() {
    return holdTime;
  }

  @JsonProperty("holdTime")
  public void setHoldTime(long holdTime) {
    this.holdTime = holdTime;
  }

  @JsonProperty("lookups")
  public long getLookups() {
    return lookups;
  }

  @JsonProperty("lookups")
  public void setLookups(long lookups) {
    this.lookups = lookups;
  }

  @JsonProperty("indexCacheHits")
  public long getIndexCacheHits() {
    return indexCacheHits;
  }

  @JsonProperty("indexCacheHits")
  public void setIndexCacheHits(long indexCacheHits) {
    this.indexCacheHits = indexCacheHits;
  }

  @JsonProperty("indexCacheRequests")
  public long getIndexCacheRequests() {
    return indexCacheRequests;
  }

  @JsonProperty("indexCacheRequests")
  public void setIndexCacheRequests(long indexCacheRequests) {
    this.indexCacheRequests = indexCacheRequests;
  }

  @JsonProperty("dataCacheHits")
  public long getDataCacheHits() {
    return dataCacheHits;
  }

  @JsonProperty("dataCacheHits")
  public void setDataCacheHits(long dataCacheHits) {
    this.dataCacheHits = dataCacheHits;
  }

  @JsonProperty("dataCacheRequests")
  public long getDataCacheRequests() {
    return dataCacheRequests;
  }

  @JsonProperty("dataCacheRequests")
  public void setDataCacheRequests(long dataCacheRequests) {
    this.dataCacheRequests = dataCacheRequests;
  }

  @JsonProperty("osLoad")
  public double getOsLoad() {
    return osLoad;
  }

  @JsonProperty("osLoad")
  public void setOsLoad(double osLoad) {
    this.osLoad = osLoad;
  }

  @JsonProperty("logRecoveries")
  public List<RecoveryStatusInformation> getLogRecoveries() {
    return logRecoveries;
  }

  @JsonProperty("logRecoveries")
  public void setLogRecoveries(List<RecoveryStatusInformation> logRecoveries) {
    this.logRecoveries = logRecoveries;
  }
}
