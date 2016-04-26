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

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.COMPACT_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.FLUSH_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Basic information needed to create a tablet.
 */
public class TabletData {
  private static Logger log = LoggerFactory.getLogger(TabletData.class);

  private String time = null;
  private SortedMap<FileRef,DataFileValue> dataFiles = new TreeMap<>();
  private List<LogEntry> logEntris = new ArrayList<>();
  private HashSet<FileRef> scanFiles = new HashSet<>();
  private long flushID = -1;
  private long compactID = -1;
  private TServerInstance lastLocation = null;
  private Map<Long,List<FileRef>> bulkImported = new HashMap<>();
  private long splitTime = 0;
  private String directory = null;

  // Read tablet data from metadata tables
  public TabletData(KeyExtent extent, VolumeManager fs, Iterator<Entry<Key,Value>> entries) {
    final Text family = new Text();
    Text rowName = extent.getMetadataEntry();
    while (entries.hasNext()) {
      Entry<Key,Value> entry = entries.next();
      Key key = entry.getKey();
      Value value = entry.getValue();
      key.getColumnFamily(family);
      if (key.compareRow(rowName) != 0) {
        log.info("Unexpected metadata table entry for {}: {}", extent, key.getRow());
        continue;
      }
      if (ServerColumnFamily.TIME_COLUMN.hasColumns(entry.getKey())) {
        if (time == null) {
          time = value.toString();
        }
      } else if (DataFileColumnFamily.NAME.equals(family)) {
        FileRef ref = new FileRef(fs, key);
        dataFiles.put(ref, new DataFileValue(entry.getValue().get()));
      } else if (DIRECTORY_COLUMN.hasColumns(key)) {
        directory = value.toString();
      } else if (family.equals(LogColumnFamily.NAME)) {
        logEntris.add(LogEntry.fromKeyValue(key, entry.getValue()));
      } else if (family.equals(ScanFileColumnFamily.NAME)) {
        scanFiles.add(new FileRef(fs, key));
      } else if (FLUSH_COLUMN.hasColumns(key)) {
        flushID = Long.parseLong(value.toString());
      } else if (COMPACT_COLUMN.hasColumns(key)) {
        compactID = Long.parseLong(entry.getValue().toString());
      } else if (family.equals(LastLocationColumnFamily.NAME)) {
        lastLocation = new TServerInstance(value, key.getColumnQualifier());
      } else if (family.equals(BulkFileColumnFamily.NAME)) {
        Long id = Long.decode(value.toString());
        List<FileRef> lst = bulkImported.get(id);
        if (lst == null) {
          bulkImported.put(id, lst = new ArrayList<>());
        }
        lst.add(new FileRef(fs, key));
      } else if (PREV_ROW_COLUMN.hasColumns(key)) {
        KeyExtent check = new KeyExtent(key.getRow(), value);
        if (!check.equals(extent)) {
          throw new RuntimeException("Found bad entry for " + extent + ": " + check);
        }
      }
    }
    if (time == null && dataFiles.isEmpty() && extent.equals(RootTable.OLD_EXTENT)) {
      // recovery... old root tablet has no data, so time doesn't matter:
      time = TabletTime.LOGICAL_TIME_ID + "" + Long.MIN_VALUE;
    }
  }

  // Read basic root table metadata from zookeeper
  public TabletData(VolumeManager fs, ZooReader rdr, AccumuloConfiguration conf) throws IOException {
    directory = VolumeUtil.switchRootTableVolume(MetadataTableUtil.getRootTabletDir());

    Path location = new Path(directory);

    // cleanReplacement() has special handling for deleting files
    FileStatus[] files = fs.listStatus(location);
    Collection<String> goodPaths = RootFiles.cleanupReplacement(fs, files, true);
    long rtime = Long.MIN_VALUE;
    for (String good : goodPaths) {
      Path path = new Path(good);
      String filename = path.getName();
      FileRef ref = new FileRef(location.toString() + "/" + filename, path);
      DataFileValue dfv = new DataFileValue(0, 0);
      dataFiles.put(ref, dfv);

      FileSystem ns = fs.getVolumeByPath(path).getFileSystem();
      FileSKVIterator reader = FileOperations.getInstance().newReaderBuilder().forFile(path.toString(), ns, ns.getConf()).withTableConfiguration(conf)
          .seekToBeginning().build();
      long maxTime = -1;
      try {
        while (reader.hasTop()) {
          maxTime = Math.max(maxTime, reader.getTopKey().getTimestamp());
          reader.next();
        }
      } finally {
        reader.close();
      }
      if (maxTime > rtime) {
        time = TabletTime.LOGICAL_TIME_ID + "" + maxTime;
        rtime = maxTime;
      }
    }

    try {
      logEntris = MetadataTableUtil.getLogEntries(null, RootTable.EXTENT);
    } catch (Exception ex) {
      throw new RuntimeException("Unable to read tablet log entries", ex);
    }
  }

  // Data pulled from an existing tablet to make a split
  public TabletData(String tabletDirectory, SortedMap<FileRef,DataFileValue> highDatafileSizes, String time, long lastFlushID, long lastCompactID,
      TServerInstance lastLocation, Map<Long,List<FileRef>> bulkIngestedFiles) {
    this.directory = tabletDirectory;
    this.dataFiles = highDatafileSizes;
    this.time = time;
    this.flushID = lastFlushID;
    this.compactID = lastCompactID;
    this.lastLocation = lastLocation;
    this.bulkImported = bulkIngestedFiles;
    this.splitTime = System.currentTimeMillis();
  }

  public static Logger getLog() {
    return log;
  }

  public String getTime() {
    return time;
  }

  public SortedMap<FileRef,DataFileValue> getDataFiles() {
    return dataFiles;
  }

  public List<LogEntry> getLogEntris() {
    return logEntris;
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
