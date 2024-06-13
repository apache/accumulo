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
package org.apache.accumulo.server.compaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionReason;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionType;

public class CompactionInfo {

  private final FileCompactor compactor;
  private final String localityGroup;
  private final long entriesRead;
  private final long entriesWritten;
  private final TCompactionReason reason;

  CompactionInfo(FileCompactor compactor) {
    this.localityGroup = compactor.getCurrentLocalityGroup();
    this.entriesRead = compactor.getEntriesRead();
    this.entriesWritten = compactor.getEntriesWritten();
    this.reason = compactor.getReason();
    this.compactor = compactor;
  }

  public long getID() {
    return compactor.getCompactorID();
  }

  public KeyExtent getExtent() {
    return compactor.getExtent();
  }

  public long getEntriesRead() {
    return entriesRead;
  }

  public long getEntriesWritten() {
    return entriesWritten;
  }

  public Thread getThread() {
    return compactor.thread;
  }

  public String getOutputFile() {
    return compactor.getOutputFile();
  }

  public ActiveCompaction toThrift() {

    TCompactionType type;

    if (compactor.hasIMM()) {
      if (!compactor.getFilesToCompact().isEmpty()) {
        type = TCompactionType.MERGE;
      } else {
        type = TCompactionType.MINOR;
      }
    } else if (!compactor.willPropagateDeletes()) {
      type = TCompactionType.FULL;
    } else {
      type = TCompactionType.MAJOR;
    }

    List<IterInfo> iiList = new ArrayList<>();
    Map<String,Map<String,String>> iterOptions = new HashMap<>();

    for (IteratorSetting iterSetting : compactor.getIterators()) {
      iiList.add(new IterInfo(iterSetting.getPriority(), iterSetting.getIteratorClass(),
          iterSetting.getName()));
      iterOptions.put(iterSetting.getName(), iterSetting.getOptions());
    }
    List<String> files = compactor.getFilesToCompact().stream().map(StoredTabletFile::getPathStr)
        .collect(Collectors.toList());
    return new ActiveCompaction(compactor.extent.toThrift(),
        System.currentTimeMillis() - compactor.getStartTime(), files, compactor.getOutputFile(),
        type, reason, localityGroup, entriesRead, entriesWritten, iiList, iterOptions);
  }
}
