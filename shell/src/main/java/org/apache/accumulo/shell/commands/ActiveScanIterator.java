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
package org.apache.accumulo.shell.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.ScanType;
import org.apache.accumulo.core.util.DurationFormat;

class ActiveScanIterator implements Iterator<String> {

  private InstanceOperations instanceOps;
  private Iterator<String> tsIter;
  private Iterator<String> scansIter;

  private void readNext() {
    final List<String> scans = new ArrayList<>();

    while (tsIter.hasNext()) {

      final String tserver = tsIter.next();
      try {
        final List<ActiveScan> asl = instanceOps.getActiveScans(tserver);

        for (ActiveScan as : asl) {
          var dur = new DurationFormat(as.getAge(), "");
          var dur2 = new DurationFormat(as.getLastContactTime(), "");
          scans.add(String.format(
              "%21s |%21s |%9s |%9s |%7s |%6s |%8s |%8s |%10s |%20s |%10s |%20s |%10s | %s",
              tserver, as.getClient(), dur, dur2, as.getState(), as.getType(), as.getUser(),
              as.getTable(), as.getColumns(), as.getAuthorizations(),
              (as.getType() == ScanType.SINGLE ? as.getTablet() : "N/A"), as.getScanid(),
              as.getSsiList(), as.getSsio()));
        }
      } catch (Exception e) {
        scans.add(tserver + " ERROR " + e.getMessage());
      }

      if (!scans.isEmpty()) {
        break;
      }
    }

    scansIter = scans.iterator();
  }

  ActiveScanIterator(List<String> tservers, InstanceOperations instanceOps) {
    this.instanceOps = instanceOps;
    this.tsIter = tservers.iterator();

    final String header = String.format(
        " %-21s| %-21s| %-9s| %-9s| %-7s| %-6s|"
            + " %-8s| %-8s| %-10s| %-20s| %-10s| %-10s | %-20s | %s",
        "TABLET SERVER", "CLIENT", "AGE", "LAST", "STATE", "TYPE", "USER", "TABLE", "COLUMNS",
        "AUTHORIZATIONS", "TABLET", "SCAN ID", "ITERATORS", "ITERATOR OPTIONS");

    scansIter = Collections.singletonList(header).iterator();
  }

  @Override
  public boolean hasNext() {
    return scansIter.hasNext();
  }

  @Override
  public String next() {
    final String next = scansIter.next();

    if (!scansIter.hasNext()) {
      readNext();
    }

    return next;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
