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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

/*
 * Basic information needed to create a tablet.
 */
public class TabletData {
  private MetadataTime time = null;
  private SortedMap<FileRef,DataFileValue> dataFiles = new TreeMap<>();
  private List<LogEntry> logEntries = new ArrayList<>();
  private HashSet<FileRef> scanFiles = new HashSet<>();
  private long flushID = -1;
  private long compactID = -1;
  private TServerInstance lastLocation = null;
  private Map<Long,List<FileRef>> bulkImported = new HashMap<>();
  private long splitTime = 0;
  private String directory = null;

  // Read tablet data from metadata tables
  public TabletData(KeyExtent extent, VolumeManager fs, TabletMetadata meta) {

    this.time = meta.getTime();
    this.compactID = meta.getCompactId().orElse(-1);
    this.flushID = meta.getFlushId().orElse(-1);
    this.directory = meta.getDir();
    this.logEntries.addAll(meta.getLogs());
    meta.getScans().forEach(path -> scanFiles.add(new FileRef(fs, path, meta.getTableId())));

    if (meta.getLast() != null)
      this.lastLocation = new TServerInstance(meta.getLast());

    meta.getFilesMap().forEach((path, dfv) -> {
      dataFiles.put(new FileRef(fs, path, meta.getTableId()), dfv);
    });

    meta.getLoaded().forEach((path, txid) -> {
      bulkImported.computeIfAbsent(txid, k -> new ArrayList<FileRef>())
          .add(new FileRef(fs, path, meta.getTableId()));
    });
  }

  // Read basic root table metadata from zookeeper
  public TabletData(ServerContext context, VolumeManager fs, AccumuloConfiguration conf,
      TabletMetadata rootMeta) throws IOException {
    Preconditions.checkArgument(rootMeta.getExtent().equals(RootTable.EXTENT));

    directory = VolumeUtil.switchRootTableVolume(context, rootMeta.getDir());

    Path location = new Path(directory);

    // cleanReplacement() has special handling for deleting files
    FileStatus[] files = fs.listStatus(location);
    Collection<String> goodPaths = RootFiles.cleanupReplacement(fs, files, true);
    long rtime = Long.MIN_VALUE;
    for (String good : goodPaths) {
      Path path = new Path(good);
      String filename = path.getName();
      FileRef ref = new FileRef(location + "/" + filename, path);
      DataFileValue dfv = new DataFileValue(0, 0);
      dataFiles.put(ref, dfv);

      FileSystem ns = fs.getVolumeByPath(path).getFileSystem();
      long maxTime = -1;
      try (FileSKVIterator reader = FileOperations.getInstance().newReaderBuilder()
          .forFile(path.toString(), ns, ns.getConf(), context.getCryptoService())
          .withTableConfiguration(conf).seekToBeginning().build()) {
        while (reader.hasTop()) {
          maxTime = Math.max(maxTime, reader.getTopKey().getTimestamp());
          reader.next();
        }
      }
      if (maxTime > rtime) {
        time = new MetadataTime(maxTime, TimeType.LOGICAL);
        rtime = maxTime;
      }
    }

    this.logEntries.addAll(rootMeta.getLogs());
  }

  // Data pulled from an existing tablet to make a split
  public TabletData(String tabletDirectory, SortedMap<FileRef,DataFileValue> highDatafileSizes,
      MetadataTime time, long lastFlushID, long lastCompactID, TServerInstance lastLocation,
      Map<Long,List<FileRef>> bulkIngestedFiles) {
    this.directory = tabletDirectory;
    this.dataFiles = highDatafileSizes;
    this.time = time;
    this.flushID = lastFlushID;
    this.compactID = lastCompactID;
    this.lastLocation = lastLocation;
    this.bulkImported = bulkIngestedFiles;
    this.splitTime = System.currentTimeMillis();
  }

  public MetadataTime getTime() {
    return time;
  }

  public SortedMap<FileRef,DataFileValue> getDataFiles() {
    return dataFiles;
  }

  public List<LogEntry> getLogEntries() {
    return logEntries;
  }

  public HashSet<FileRef> getScanFiles() {
    return scanFiles;
  }

  public long getFlushID() {
    return flushID;
  }

  public long getCompactID() {
    return compactID;
  }

  public TServerInstance getLastLocation() {
    return lastLocation;
  }

  public Map<Long,List<FileRef>> getBulkImported() {
    return bulkImported;
  }

  public String getDirectory() {
    return directory;
  }

  public long getSplitTime() {
    return splitTime;
  }
}
