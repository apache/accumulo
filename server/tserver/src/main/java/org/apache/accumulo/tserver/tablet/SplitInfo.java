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
package org.apache.accumulo.tserver.tablet;

import java.util.SortedMap;

import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.master.state.TServerInstance;

/**
 * operations are disallowed while we split which is ok since splitting is fast
 *
 * a minor compaction should have taken place before calling this so there should be relatively little left to compact
 *
 * we just need to make sure major compactions aren't occurring if we have the major compactor thread decide who needs splitting we can avoid synchronization
 * issues with major compactions
 *
 */

final public class SplitInfo {
  private final String dir;
  private final SortedMap<FileRef,DataFileValue> datafiles;
  private final String time;
  private final long initFlushID;
  private final long initCompactID;
  private final TServerInstance lastLocation;

  SplitInfo(String d, SortedMap<FileRef,DataFileValue> dfv, String time, long initFlushID, long initCompactID, TServerInstance lastLocation) {
    this.dir = d;
    this.datafiles = dfv;
    this.time = time;
    this.initFlushID = initFlushID;
    this.initCompactID = initCompactID;
    this.lastLocation = lastLocation;
  }

  public String getDir() {
    return dir;
  }

  public SortedMap<FileRef,DataFileValue> getDatafiles() {
    return datafiles;
  }

  public String getTime() {
    return time;
  }

  public long getInitFlushID() {
    return initFlushID;
  }

  public long getInitCompactID() {
    return initCompactID;
  }

  public TServerInstance getLastLocation() {
    return lastLocation;
  }

}
