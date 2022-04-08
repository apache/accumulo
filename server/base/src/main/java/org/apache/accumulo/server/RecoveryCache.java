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
package org.apache.accumulo.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RecoveryCache {
  private final Path recoveryDir;
  private final List<FileSKVIterator> scanners;
  private final List<Path> logFiles;
  private final CacheProvider cacheProvider;

  public RecoveryCache(ServerContext context, CacheProvider cacheProvider, Path recoveryDir)
      throws IOException {
    this.recoveryDir = recoveryDir;
    this.scanners = new ArrayList<>();
    this.logFiles = new ArrayList<>();
    this.cacheProvider = cacheProvider;
    setupRecoveryCache(context, cacheProvider);
  }

  /**
   * Create the Scanners for each WAL in the recovery directory.
   */
  public void setupRecoveryCache(ServerContext context, CacheProvider cacheProvider)
      throws IOException {
    List<FileSKVIterator> recoveryScanners = new ArrayList<>();
    var conf = context.getConfiguration();

    logFiles.addAll(getWALFiles(context.getVolumeManager(), recoveryDir));
    for (var wal : logFiles) {
      var fs = context.getVolumeManager().getFileSystemByPath(wal);
      // ScannerImpl recoveryFileScanner = new ScannerImpl(context, tableId, new Authorizations());
      // CacheProvider cacheProvider = new BasicCacheProvider(null, null/*TODO*/);

      var fileIterator =
          FileOperations.getInstance().newReaderBuilder().withCacheProvider(cacheProvider)
              .forFile(wal.getName(), fs, context.getHadoopConf(), context.getCryptoService())
              .withTableConfiguration(conf).seekToBeginning().build();

      recoveryScanners.add(fileIterator);
    }
    scanners.addAll(recoveryScanners);
  }

  /**
   * Check for sorting signal files (finished/failed) and get the logs in the provided directory.
   */
  private List<Path> getWALFiles(VolumeManager fs, Path directory) throws IOException {
    boolean foundFinish = false;
    List<Path> logFiles = new ArrayList<>();
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
      logFiles.add(fullLogPath);
    }
    if (!foundFinish)
      throw new IOException(
          "Sort '" + SortedLogState.FINISHED.getMarker() + "' flag not found in " + directory);
    return logFiles;
  }

  public List<Path> getLogFiles() {
    return logFiles;
  }

  public List<FileSKVIterator> getScanners() {
    return scanners;
  }

  public void close() throws IOException {
    for (FileSKVIterator scanner : scanners) {
      scanner.close();
    }
  }

  public CacheProvider getCacheProvider() {
    return cacheProvider;
  }
}
