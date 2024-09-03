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
package org.apache.accumulo.server.constraints;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.metadata.StoredTabletFile;

/**
 * Data needed to validate a BulkFileColumn update
 */
class BulkFileColData {
  private boolean isSplitMutation, isLocationMutation;
  private final Set<StoredTabletFile> dataFiles, loadedFiles;
  private final Set<String> tidsSeen;

  BulkFileColData() {
    isSplitMutation = false;
    isLocationMutation = false;
    dataFiles = new HashSet<>();
    loadedFiles = new HashSet<>();
    tidsSeen = new HashSet<>();
  }

  void setIsSplitMutation(boolean b) {
    isSplitMutation = b;
  }

  boolean getIsSplitMutation() {
    return isSplitMutation;
  }

  void setIsLocationMutation(boolean b) {
    isLocationMutation = b;
  }

  boolean getIsLocationMutation() {
    return isLocationMutation;
  }

  void addDataFile(StoredTabletFile stf) {
    dataFiles.add(stf);
  }

  void addLoadedFile(StoredTabletFile stf) {
    loadedFiles.add(stf);
  }

  boolean dataFilesEqualsLoadedFiles() {
    return dataFiles.equals(loadedFiles);
  }

  void addTidSeen(String tid) {
    tidsSeen.add(tid);
  }

  Set<String> getTidsSeen() {
    return tidsSeen;
  }
}
