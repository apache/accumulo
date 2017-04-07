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
package org.apache.accumulo.server.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.master.thrift.BulkImportStatus;

// A little class to hold bulk import status information in the Master
// and two places in the tablet server.
public class ServerBulkImportStatus {
  private final ConcurrentMap<String,BulkImportStatus> status = new ConcurrentHashMap<>();

  public List<BulkImportStatus> getBulkLoadStatus() {
    return new ArrayList<>(status.values());
  }

  public void updateBulkImportStatus(List<String> files, BulkImportState state) {
    for (String file : files) {
      BulkImportStatus initial = new BulkImportStatus(System.currentTimeMillis(), file, state);
      status.putIfAbsent(file, initial);
      initial = status.get(file);
      if (initial != null) {
        initial.state = state;
      }
    }
  }

  public void removeBulkImportStatus(List<String> files) {
    status.keySet().removeAll(files);
  }

}
