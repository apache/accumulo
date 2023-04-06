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
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.master.thrift.BulkImportStatus;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.util.DurationFormat;

public class BulkImportListIterator implements Iterator<String> {

  private final Iterator<String> iter;

  public BulkImportListIterator(List<String> tservers, ManagerMonitorInfo stats) {
    List<String> result = new ArrayList<>();
    for (BulkImportStatus status : stats.getBulkImports()) {
      result.add(format(status));
    }
    if (!tservers.isEmpty()) {
      for (TabletServerStatus tserver : stats.getTServerInfo()) {
        if (tservers.contains(tserver.name)) {
          result.add(tserver.name + ":");
          for (BulkImportStatus status : tserver.bulkImports) {
            result.add(format(status));
          }
        }
      }
    }
    iter = result.iterator();
  }

  private String format(BulkImportStatus status) {
    long diff = System.currentTimeMillis() - status.startTime;
    var dur = new DurationFormat(diff, " ");
    return String.format("%25s | %4s | %s", status.filename, dur, status.state);
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public String next() {
    return iter.next();
  }

  @Override
  public void remove() {
    iter.remove();
  }

}
