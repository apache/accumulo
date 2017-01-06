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
package org.apache.accumulo.shell.commands;

import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.TServerStatus;
import org.apache.accumulo.core.client.admin.InstanceOperations;

class TabletServerStatusIterator implements Iterator<String> {

  private Iterator<TServerStatus> iter;
  private InstanceOperations instanceOps;

  TabletServerStatusIterator(List<TServerStatus> tservers, InstanceOperations instanceOps) {
    iter = tservers.iterator();
    this.instanceOps = instanceOps;
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public String next() {
    TServerStatus tserver = iter.next();
    long now = System.currentTimeMillis();
    String name, hostedTablets, lastContact, entries, ingest, query, holdTime, scans, minor, major, indexHitRate, dataHitRate,
      osLoad, version;
    
    name = tserver.getName();
    hostedTablets = Integer.toString(tserver.getHostedTablets());
    lastContact = Long.toString(now - tserver.getLastContact());
    entries = Long.toString(tserver.getEntries());
    ingest = Double.toString(tserver.getIngest());
    query = Double.toString(tserver.getQuery());
    holdTime = Long.toString(tserver.getHoldTime());
    scans = Integer.toString(tserver.getScans());
    minor = Integer.toString(tserver.getMinor());
    major = Integer.toString(tserver.getMajor());
    indexHitRate = Double.toString(tserver.getIndexHitRate());
    dataHitRate = Double.toString(tserver.getDataHitRate());
    osLoad = Double.toString(tserver.getOsLoad());
    version = tserver.getVersion();
    
    // Check config file for this
    
    return "\n-----------------------+------------------" +
           "\nNAME                   | VALUE            " +
           "\n-----------------------+------------------" +
           "\nServer ............... | " + name + 
           "\nHosted Tablets ....... | " + hostedTablets + 
           "\nLast Contact ......... |" + lastContact + 
           "\nEntries .............. | " + entries + 
           "\nIngest ............... | " + ingest + 
           "\nQuery ................ | " + query + 
           "\nHold Time ............ | " + holdTime + 
           "\nRunning Scans ........ | " + scans + 
           "\nMinor Compactions .... | " + minor + 
           "\nMajor Compactions .... | " + major + 
           "\nIndex Cache Hit Rate . | " + indexHitRate + 
           "\nData Cache Hit Rate .. | " + dataHitRate + 
           "\nOS Load .............. | " + osLoad + 
           "\nVersion .............. | " + version;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
