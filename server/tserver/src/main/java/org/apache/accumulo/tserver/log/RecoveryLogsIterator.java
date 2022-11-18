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
package org.apache.accumulo.tserver.log;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.crypto.CryptoEnvironmentImpl;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.IteratorAdapter;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

/**
 * Iterates over multiple sorted recovery logs merging them into a single sorted stream.
 */
public class RecoveryLogsIterator
    implements Iterator<Entry<LogFileKey,LogFileValue>>, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(RecoveryLogsIterator.class);

  private final List<FileSKVIterator> fileIters;
  private final Iterator<Entry<Key,Value>> iter;
  private final CryptoEnvironment env = new CryptoEnvironmentImpl(CryptoEnvironment.Scope.RECOVERY);

  /**
   * Scans the files in each recoveryLogDir over the range [start,end].
   */
  public RecoveryLogsIterator(ServerContext context, List<Path> recoveryLogDirs, LogFileKey start,
      LogFileKey end, boolean checkFirstKey) throws IOException {

    List<Iterator<Entry<Key,Value>>> iterators = new ArrayList<>(recoveryLogDirs.size());
    fileIters = new ArrayList<>();
    Range range = start == null ? null : LogFileKey.toRange(start, end);
    var vm = context.getVolumeManager();

    final CryptoService cryptoService = context.getCryptoFactory().getService(env,
        context.getConfiguration().getAllCryptoProperties());

    for (Path logDir : recoveryLogDirs) {
      LOG.debug("Opening recovery log dir {}", logDir.getName());
      SortedSet<Path> logFiles = getFiles(vm, logDir);
      var fs = vm.getFileSystemByPath(logDir);

      // only check the first key once to prevent extra iterator creation and seeking
      if (checkFirstKey && !logFiles.isEmpty()) {
        validateFirstKey(context, cryptoService, fs, logFiles, logDir);
      }

      for (Path log : logFiles) {
        FileSKVIterator fileIter = FileOperations.getInstance().newReaderBuilder()
            .forFile(log.toString(), fs, fs.getConf(), cryptoService)
            .withTableConfiguration(context.getConfiguration()).seekToBeginning().build();
        if (range != null) {
          fileIter.seek(range, Collections.emptySet(), false);
        }
        Iterator<Entry<Key,Value>> scanIter = new IteratorAdapter(fileIter);

        if (scanIter.hasNext()) {
          LOG.debug("Write ahead log {} has data in range {} {}", log.getName(), start, end);
          iterators.add(scanIter);
          fileIters.add(fileIter);
        } else {
          LOG.debug("Write ahead log {} has no data in range {} {}", log.getName(), start, end);
          fileIter.close();
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
  public Entry<LogFileKey,LogFileValue> next() {
    Entry<Key,Value> e = iter.next();
    return new AbstractMap.SimpleImmutableEntry<>(LogFileKey.fromKey(e.getKey()),
        LogFileValue.fromValue(e.getValue()));
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  @Override
  public void close() throws IOException {
    for (FileSKVIterator fskv : fileIters) {
      fskv.close();
    }
  }

  /**
   * Check for sorting signal files (finished/failed) and get the logs in the provided directory.
   */
  private SortedSet<Path> getFiles(VolumeManager fs, Path directory) throws IOException {
    boolean foundFinish = false;
    // Path::getName compares the last component of each Path value. In this case, the last
    // component should
    // always have the format 'part-r-XXXXX.rf', where XXXXX are one-up values.
    SortedSet<Path> logFiles = new TreeSet<>(Comparator.comparing(Path::getName));
    for (FileStatus child : fs.listStatus(directory)) {
      if (child.getPath().getName().startsWith("_")) {
        continue;
      }
      if (SortedLogState.isFinished(child.getPath().getName())) {
        foundFinish = true;
        continue;
      }
      if (SortedLogState.FAILED.getMarker().equals(child.getPath().getName())) {
        continue;
      }
      FileSystem ns = fs.getFileSystemByPath(child.getPath());
      Path fullLogPath = ns.makeQualified(child.getPath());
      logFiles.add(fullLogPath);
    }
    if (!foundFinish) {
      throw new IOException(
          "Sort '" + SortedLogState.FINISHED.getMarker() + "' flag not found in " + directory);
    }
    return logFiles;
  }

  /**
   * Check that the first entry in the WAL is OPEN. Only need to do this once.
   */
  private void validateFirstKey(ServerContext context, CryptoService cs, FileSystem fs,
      SortedSet<Path> logFiles, Path fullLogPath) throws IOException {
    try (FileSKVIterator fileIter = FileOperations.getInstance().newReaderBuilder()
        .forFile(logFiles.first().toString(), fs, fs.getConf(), cs)
        .withTableConfiguration(context.getConfiguration()).seekToBeginning().build()) {
      Iterator<Entry<Key,Value>> iterator = new IteratorAdapter(fileIter);

      if (iterator.hasNext()) {
        Key firstKey = iterator.next().getKey();
        LogFileKey key = LogFileKey.fromKey(firstKey);
        if (key.event != LogEvents.OPEN) {
          throw new IllegalStateException("First log entry is not OPEN " + fullLogPath);
        }
      }
    }
  }
}
