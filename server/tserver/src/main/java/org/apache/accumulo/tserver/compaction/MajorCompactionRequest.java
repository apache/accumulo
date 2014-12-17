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
package org.apache.accumulo.tserver.compaction;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Information that can be used to determine how a tablet is to be major compacted, if needed.
 */
public class MajorCompactionRequest implements Cloneable {
  final private KeyExtent extent;
  final private MajorCompactionReason reason;
  final private VolumeManager volumeManager;
  final private AccumuloConfiguration tableConfig;
  private Map<FileRef,DataFileValue> files;

  public MajorCompactionRequest(KeyExtent extent, MajorCompactionReason reason, VolumeManager manager, AccumuloConfiguration tabletConfig) {
    this.extent = extent;
    this.reason = reason;
    this.volumeManager = manager;
    this.tableConfig = tabletConfig;
    this.files = Collections.emptyMap();
  }

  public MajorCompactionRequest(MajorCompactionRequest mcr) {
    this(mcr.extent, mcr.reason, mcr.volumeManager, mcr.tableConfig);
    // know this is already unmodifiable, no need to wrap again
    this.files = mcr.files;
  }

  public KeyExtent getExtent() {
    return extent;
  }

  public MajorCompactionReason getReason() {
    return reason;
  }

  public Map<FileRef,DataFileValue> getFiles() {
    return files;
  }

  public void setFiles(Map<FileRef,DataFileValue> update) {
    this.files = Collections.unmodifiableMap(update);
  }

  public FileStatus[] listStatus(Path path) throws IOException {
    // @TODO verify the file isn't some random file in HDFS
    return volumeManager.listStatus(path);
  }

  public FileSKVIterator openReader(FileRef ref) throws IOException {
    // @TODO verify the file isn't some random file in HDFS
    // @TODO ensure these files are always closed?
    FileOperations fileFactory = FileOperations.getInstance();
    FileSystem ns = volumeManager.getVolumeByPath(ref.path()).getFileSystem();
    FileSKVIterator openReader = fileFactory.openReader(ref.path().toString(), true, ns, ns.getConf(), tableConfig);
    return openReader;
  }

  public Map<String,String> getTableProperties() {
    return tableConfig.getAllPropertiesWithPrefix(Property.TABLE_PREFIX);
  }

  public String getTableConfig(String key) {
    Property property = Property.getPropertyByKey(key);
    if (property == null || property.isSensitive())
      throw new RuntimeException("Unable to access the configuration value " + key);
    return tableConfig.get(property);
  }

  public int getMaxFilesPerTablet() {
    return tableConfig.getMaxFilesPerTablet();
  }
}
