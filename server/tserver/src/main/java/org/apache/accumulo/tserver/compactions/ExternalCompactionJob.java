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
package org.apache.accumulo.tserver.compactions;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.InputFile;
import org.apache.accumulo.core.tabletserver.thrift.IteratorConfig;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;

import com.google.common.base.Preconditions;

public class ExternalCompactionJob {

  private Map<StoredTabletFile,DataFileValue> jobFiles;
  private boolean propagateDeletes;
  private TabletFile compactTmpName;
  private KeyExtent extent;
  private ExternalCompactionId externalCompactionId;
  private CompactionKind kind;
  private List<IteratorSetting> iters;
  private long userCompactionId;
  private Map<String,String> overrides;

  public ExternalCompactionJob() {}

  public ExternalCompactionJob(Map<StoredTabletFile,DataFileValue> jobFiles,
      boolean propagateDeletes, TabletFile compactTmpName, KeyExtent extent,
      ExternalCompactionId externalCompactionId, CompactionKind kind, List<IteratorSetting> iters,
      Long userCompactionId, Map<String,String> overrides) {
    this.jobFiles = Objects.requireNonNull(jobFiles);
    this.propagateDeletes = propagateDeletes;
    this.compactTmpName = Objects.requireNonNull(compactTmpName);
    this.extent = Objects.requireNonNull(extent);
    this.externalCompactionId = Objects.requireNonNull(externalCompactionId);
    this.kind = Objects.requireNonNull(kind);
    this.iters = Objects.requireNonNull(iters);
    if (kind == CompactionKind.USER) {
      Preconditions.checkArgument(userCompactionId != null && userCompactionId > 0);
      this.userCompactionId = userCompactionId;
    } else {
      this.userCompactionId = 0;
    }
    this.overrides = overrides;
  }

  public TExternalCompactionJob toThrift() {
    IteratorConfig iteratorSettings = SystemIteratorUtil.toIteratorConfig(iters);

    List<InputFile> files = jobFiles.entrySet().stream().map(e -> {
      var dfv = e.getValue();
      return new InputFile(e.getKey().getPathStr(), dfv.getSize(), dfv.getNumEntries(),
          dfv.getTime());
    }).collect(Collectors.toList());

    return new TExternalCompactionJob(externalCompactionId.toString(), extent.toThrift(), files,
        iteratorSettings, compactTmpName.getPathStr(), propagateDeletes,
        org.apache.accumulo.core.tabletserver.thrift.TCompactionKind.valueOf(kind.name()),
        userCompactionId, overrides);
  }

  public ExternalCompactionId getExternalCompactionId() {
    return externalCompactionId;
  }

  public KeyExtent getExtent() {
    return extent;
  }
}
