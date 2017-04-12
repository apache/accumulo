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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.tabletserver.thrift.CompactionReason;
import org.apache.accumulo.core.tabletserver.thrift.CompactionType;
import org.apache.accumulo.server.fs.FileRef;

public class CompactionInfo {

  private final Compactor compactor;
  private final String localityGroup;
  private final long entriesRead;
  private final long entriesWritten;

  CompactionInfo(Compactor compactor) {
    this.localityGroup = compactor.getCurrentLocalityGroup();
    this.entriesRead = compactor.getEntriesRead();
    this.entriesWritten = compactor.getEntriesWritten();
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

    CompactionType type;

    if (compactor.hasIMM())
      if (compactor.getFilesToCompact().size() > 0)
        type = CompactionType.MERGE;
      else
        type = CompactionType.MINOR;
    else if (!compactor.willPropogateDeletes())
      type = CompactionType.FULL;
    else
      type = CompactionType.MAJOR;

    CompactionReason reason;

    if (compactor.hasIMM()) {
      switch (compactor.getMinCReason()) {
        case USER:
          reason = CompactionReason.USER;
          break;
        case CLOSE:
          reason = CompactionReason.CLOSE;
          break;
        case SYSTEM:
        default:
          reason = CompactionReason.SYSTEM;
          break;
      }
    } else {
      switch (compactor.getMajorCompactionReason()) {
        case USER:
          reason = CompactionReason.USER;
          break;
        case CHOP:
          reason = CompactionReason.CHOP;
          break;
        case IDLE:
          reason = CompactionReason.IDLE;
          break;
        case NORMAL:
        default:
          reason = CompactionReason.SYSTEM;
          break;
      }
    }

    List<IterInfo> iiList = new ArrayList<>();
    Map<String,Map<String,String>> iterOptions = new HashMap<>();

    for (IteratorSetting iterSetting : compactor.getIterators()) {
      iiList.add(new IterInfo(iterSetting.getPriority(), iterSetting.getIteratorClass(), iterSetting.getName()));
      iterOptions.put(iterSetting.getName(), iterSetting.getOptions());
    }
    List<String> filesToCompact = new ArrayList<>();
    for (FileRef ref : compactor.getFilesToCompact())
      filesToCompact.add(ref.toString());
    return new ActiveCompaction(compactor.extent.toThrift(), System.currentTimeMillis() - compactor.getStartTime(), filesToCompact, compactor.getOutputFile(),
        type, reason, localityGroup, entriesRead, entriesWritten, iiList, iterOptions);
  }
}
