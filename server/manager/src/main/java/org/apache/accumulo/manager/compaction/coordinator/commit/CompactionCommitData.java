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
package org.apache.accumulo.manager.compaction.coordinator.commit;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;

public class CompactionCommitData implements Serializable {
  private static final long serialVersionUID = 1L;
  final CompactionKind kind;
  final Set<String> inputPaths;
  final String outputTmpPath;
  final String ecid;
  final TKeyExtent textent;
  final TCompactionStats stats;

  public CompactionCommitData(ExternalCompactionId ecid, KeyExtent extent, CompactionMetadata ecm,
      TCompactionStats stats) {
    this.ecid = ecid.canonical();
    this.textent = extent.toThrift();
    this.kind = ecm.getKind();
    this.inputPaths =
        ecm.getJobFiles().stream().map(StoredTabletFile::getMetadata).collect(Collectors.toSet());
    this.outputTmpPath = ecm.getCompactTmpName().getNormalizedPathStr();
    this.stats = stats;
  }

  public TableId getTableId() {
    return KeyExtent.fromThrift(textent).tableId();
  }

  public Collection<StoredTabletFile> getJobFiles() {
    return inputPaths.stream().map(StoredTabletFile::of).collect(Collectors.toList());
  }
}
