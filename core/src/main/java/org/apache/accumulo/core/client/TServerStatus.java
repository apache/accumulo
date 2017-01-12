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
package org.apache.accumulo.core.client;

/**
 * This exception is thrown if a table is deleted after an operation starts.
 *
 * For example if table A exist when a scan is started, but is deleted during the scan then this exception is thrown.
 *
 */

public class TServerStatus {

  private String name, version;
  int hostedTablets;
  long lastContact, entries, holdTime;
  double ingest, query, indexHitRate, dataHitRate, osLoad;
  Integer scans, minor, major;

  public TServerStatus() {
    name = "";
    hostedTablets = 0;
    lastContact = 0l;
    entries = 0l;
    ingest = 0.0;
    query = 0.0;
    holdTime = 0l;
    scans = null;
    minor = null;
    major = null;
    indexHitRate = 0.0;
    dataHitRate = 0.0;
    osLoad = 0.0;
    version = "";
  }

  public TServerStatus(String name, int hostedTablets, long lastContact, long entries, double ingest, double query, long holdTime, Integer scans,
      Integer minor, Integer major, double indexHitRate, double dataHitRate, double osLoad, String version) {
    this.name = name;
    this.hostedTablets = hostedTablets;
    this.lastContact = lastContact;
    this.entries = entries;
    this.ingest = ingest;
    this.query = query;
    this.holdTime = holdTime;
    this.scans = scans;
    this.minor = minor;
    this.major = major;
    this.indexHitRate = indexHitRate;
    this.dataHitRate = dataHitRate;
    this.osLoad = osLoad;
    this.version = version;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public int getHostedTablets() {
    return hostedTablets;
  }

  public void setHostedTablets(int hostedTablets) {
    this.hostedTablets = hostedTablets;
  }

  public long getLastContact() {
    return lastContact;
  }

  public void setLastContact(long lastContact) {
    this.lastContact = lastContact;
  }

  public long getEntries() {
    return entries;
  }

  public void setEntries(long entries) {
    this.entries = entries;
  }

  public long getHoldTime() {
    return holdTime;
  }

  public void setHoldTime(long holdTime) {
    this.holdTime = holdTime;
  }

  public double getIngest() {
    return ingest;
  }

  public void setIngest(double ingest) {
    this.ingest = ingest;
  }

  public double getQuery() {
    return query;
  }

  public void setQuery(double query) {
    this.query = query;
  }

  public double getIndexHitRate() {
    return indexHitRate;
  }

  public void setIndexHitRate(double indexHitRate) {
    this.indexHitRate = indexHitRate;
  }

  public double getDataHitRate() {
    return dataHitRate;
  }

  public void setDataHitRate(double dataHitRate) {
    this.dataHitRate = dataHitRate;
  }

  public double getOsLoad() {
    return osLoad;
  }

  public void setOsLoad(double osLoad) {
    this.osLoad = osLoad;
  }

  public Integer getScans() {
    return scans;
  }

  public void setScans(Integer scans) {
    this.scans = scans;
  }

  public Integer getMinor() {
    return minor;
  }

  public void setMinor(Integer minor) {
    this.minor = minor;
  }

  public Integer getMajor() {
    return major;
  }

  public void setMajor(Integer major) {
    this.major = major;
  }

}
