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
package org.apache.accumulo.core.metadata.schema;

import static java.util.stream.Collectors.toList;
import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.metadata.StoredTabletFile;

import com.google.common.base.Preconditions;

/**
 * This class is used to manage the set of files selected for a user compaction for a tablet.
 */
public class SelectedFiles {

  private final Set<StoredTabletFile> files;

  private final boolean initiallySelectedAll;
  private final long fateTxId;
  private final String metadataValue;

  // This class is used to serialize and deserialize this class using GSon. Any changes to this
  // class must consider persisted data.
  private static class GSonData {
    String txid;
    boolean selAll;
    List<String> files;
  }

  public SelectedFiles(Set<StoredTabletFile> files, boolean initiallySelectedAll, long fateTxId) {
    Preconditions.checkArgument(files != null && !files.isEmpty());
    this.files = Set.copyOf(files);
    this.initiallySelectedAll = initiallySelectedAll;
    this.fateTxId = fateTxId;

    GSonData jData = new GSonData();
    // sort to make the serialized version equals when the sets are equal
    jData.files =
        files.stream().map(StoredTabletFile::getMetaUpdateDelete).sorted().collect(toList());
    jData.txid = FateTxId.formatTid(fateTxId);
    jData.selAll = initiallySelectedAll;
    // ELASITICITY_TODO need the produced json to always be the same when the input data is the same
    // as its used for comparison. Need unit test to ensure this behavior.
    metadataValue = GSON.get().toJson(jData);
  }

  private SelectedFiles(Set<StoredTabletFile> files, boolean initiallySelectedAll, long fateTxId,
      String metaVal) {
    Preconditions.checkArgument(files != null && !files.isEmpty());
    this.files = files;
    this.initiallySelectedAll = initiallySelectedAll;
    this.fateTxId = fateTxId;
    this.metadataValue = metaVal;
  }

  public static SelectedFiles from(String json) {
    GSonData jData = GSON.get().fromJson(json, GSonData.class);
    return new SelectedFiles(
        jData.files.stream().map(StoredTabletFile::new).collect(Collectors.toSet()), jData.selAll,
        FateTxId.fromString(jData.txid), json);
  }

  public Set<StoredTabletFile> getFiles() {
    return files;
  }

  public boolean initiallySelectedAll() {
    return initiallySelectedAll;
  }

  public long getFateTxId() {
    return fateTxId;
  }

  public String getMetadataValue() {
    return metadataValue;
  }

}
