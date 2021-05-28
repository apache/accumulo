/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

/**
 * Iterates over multiple sorted recovery logs merging them into a single sorted stream.
 */
public class RecoveryLogsIterator implements Iterator<Entry<Key,Value>>, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(RecoveryLogsIterator.class);

  private final List<Scanner> scanners;
  private final Iterator<Entry<Key,Value>> iter;

  /**
   * Scans the files in each recoveryLogDir over the range [start,end].
   */
  RecoveryLogsIterator(VolumeManager vm, List<Path> recoveryLogDirs, LogFileKey start,
      LogFileKey end, boolean checkFirstKey, LogEvents... colFamToFetch) throws IOException {

    List<Iterator<Entry<Key,Value>>> iterators = new ArrayList<>(recoveryLogDirs.size());
    scanners = new ArrayList<>();
    Key startKey = start.toKey();
    Key endKey = end.toKey();
    Range range = new Range(startKey, endKey);

    for (Path logDir : recoveryLogDirs) {
      LOG.debug("Opening recovery log dir {}", logDir.getName());
      String[] logFiles = getFiles(vm, logDir);
      var fs = vm.getFileSystemByPath(logDir);

      // only check the first key once to prevent extra iterator creation and seeking
      if (checkFirstKey) {
        validateFirstKey(RFile.newScanner().from(logFiles).withFileSystem(fs).build(), logDir);
      }

      for (String log : logFiles) {
        var scanner = RFile.newScanner().from(log).withFileSystem(fs).build();
        for (var cf : colFamToFetch)
          scanner.fetchColumnFamily(new Text(cf.name()));
        scanner.setRange(range);
        LOG.debug("Get iterator for Key Range = {} to {}", startKey, endKey);
        Iterator<Entry<Key,Value>> scanIter = scanner.iterator();

        if (scanIter.hasNext()) {
          LOG.debug("Write ahead log {} has data in range {} {}", log, start, end);
          iterators.add(scanIter);
          scanners.add(scanner);
        } else {
          LOG.debug("Write ahead log {} has no data in range {} {}", log, start, end);
          scanner.close();
        }
      }
    }
    iter = Iterators.mergeSorted(iterators, Entry.comparingByKey());
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public Entry<Key,Value> next() {
    return iter.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  @Override
  public void close() {
    scanners.forEach(ScannerBase::close);
  }

  /**
   * Check for sorting signal files (finished/failed) and get the logs in the provided directory.
   */
  private String[] getFiles(VolumeManager fs, Path directory) throws IOException {
    boolean foundFinish = false;
    List<String> logFiles = new ArrayList<>();
    for (FileStatus child : fs.listStatus(directory)) {
      if (child.getPath().getName().startsWith("_"))
        continue;
      if (SortedLogState.isFinished(child.getPath().getName())) {
        foundFinish = true;
        continue;
      }
      if (SortedLogState.FAILED.getMarker().equals(child.getPath().getName())) {
        continue;
      }
      FileSystem ns = fs.getFileSystemByPath(child.getPath());
      Path fullLogPath = ns.makeQualified(child.getPath());
      logFiles.add(fullLogPath.toString());
    }
    if (!foundFinish)
      throw new IOException(
          "Sort \"" + SortedLogState.FINISHED.getMarker() + "\" flag not found in " + directory);
    return logFiles.toArray(new String[0]);
  }

  /**
   * Check that the first entry in the WAL is OPEN. Only need to do this once.
   */
  private void validateFirstKey(Scanner scanner, Path fullLogPath) throws IOException {
    Iterator<Entry<Key,Value>> iterator = scanner.iterator();
    if (iterator.hasNext()) {
      Key firstKey = iterator.next().getKey();
      LogFileKey key = LogFileKey.fromKey(firstKey);
      if (key.event != LogEvents.OPEN) {
        throw new IllegalStateException("First log entry is not OPEN " + fullLogPath);
      }
    }
  }
}
