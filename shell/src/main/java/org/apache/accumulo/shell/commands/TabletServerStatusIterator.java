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
import org.apache.accumulo.shell.Shell;

import jline.console.ConsoleReader;

class TabletServerStatusIterator implements Iterator<String> {

  private Iterator<TServerStatus> iter;
  private int COL1 = 25;
  private ConsoleReader reader;

  TabletServerStatusIterator(List<TServerStatus> tservers, Shell shellState) {
    iter = tservers.iterator();
    this.reader = shellState.getReader();
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
    
    String output = "";

    output += printStatHeader();
    output += printStatLine("Server", name);
    output += printStatLine("Hosted Tablets", hostedTablets);
    output += printStatLine("Last Contact", lastContact);
    output += printStatLine("Entries", entries);
    output += printStatLine("Ingest", ingest);
    output += printStatLine("Query", query);
    output += printStatLine("Hold Time", holdTime);
    output += printStatLine("Running Scans", scans);
    output += printStatLine("Minor Compactions", minor);
    output += printStatLine("Major Compactions", major);
    output += printStatLine("Index Cache Hit Rate", indexHitRate);
    output += printStatLine("Data Cache Hit Rate", dataHitRate);
    output += printStatLine("OS Load", osLoad);
    output += printStatLine("Version", version);
    
    output += printStatFooter();
    
    return output;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
  
  private String printStatHeader() {
    String output = "";
    output += printStatFooter();
    output += (String.format("\n%-" + COL1 + "s | %s", "NAME", "VALUE"));
    output += printStatFooter();
    
    return output;
  }
  
  private String printStatLine(String s1, String s2) {
    String output = "";
    if (s1.length() < COL1) { 
      s1 += " " + Shell.repeat(".", COL1 - s1.length() - 1);
    }
    output += String.format("\n%-" + COL1 + "s | %s", s1, s2.replace("\n", "\n" + Shell.repeat(" ", COL1 + 1) + "| "));
    
    return output;
  }
  
  private String printStatFooter() {
    String output = "";
    int col2 = Math.max(1,  Math.min(Integer.MAX_VALUE, reader.getTerminal().getWidth() - COL1 - 6));
    output += String.format("\n%" + COL1 + "s-+-%-" + col2 + "s", Shell.repeat("-", COL1), Shell.repeat("-", col2));
    
    return output;
  }

}
