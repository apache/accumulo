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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue.Processor;
import org.apache.accumulo.tserver.log.DfsLogger.LogHeaderIncompleteException;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class LogSorter {

  private static final Logger log = LoggerFactory.getLogger(LogSorter.class);
  AccumuloConfiguration conf;

  private final Map<String,LogProcessor> currentWork = Collections.synchronizedMap(new HashMap<>());

  class LogProcessor implements Processor {

    private FSDataInputStream input;
    private DataInputStream decryptingInput;
    private long bytesCopied = -1;
    private long sortStart = 0;
    private long sortStop = -1;

    @Override
    public Processor newProcessor() {
      return new LogProcessor();
    }

    @Override
    public void process(String child, byte[] data) {
      String work = new String(data);
      String[] parts = work.split("\\|");
      String src = parts[0];
      String dest = parts[1];
      String sortId = new Path(src).getName();
      log.debug("Sorting {} to {} using sortId {}", src, dest, sortId);

      synchronized (currentWork) {
        if (currentWork.containsKey(sortId))
          return;
        currentWork.put(sortId, this);
      }

      synchronized (this) {
        sortStart = System.currentTimeMillis();
      }

      VolumeManager fs = context.getVolumeManager();

      String formerThreadName = Thread.currentThread().getName();
      try {
        sort(fs, sortId, new Path(src), dest);
      } catch (Exception t) {
        try {
          // parent dir may not exist
          fs.mkdirs(new Path(dest));
          fs.create(SortedLogState.getFailedMarkerPath(dest)).close();
        } catch (IOException e) {
          log.error("Error creating failed flag file " + sortId, e);
        }
        log.error("Caught exception", t);
      } finally {
        Thread.currentThread().setName(formerThreadName);
        try {
          close();
        } catch (Exception e) {
          log.error("Error during cleanup sort/copy " + sortId, e);
        }
        synchronized (this) {
          sortStop = System.currentTimeMillis();
        }
        currentWork.remove(sortId);
      }
    }

    public void sort(VolumeManager fs, String name, Path srcPath, String destPath)
        throws IOException {
      int part = 0;

      // check for finished first since another thread may have already done the sort
      if (fs.exists(SortedLogState.getFinishedMarkerPath(destPath))) {
        log.debug("Sorting already finished at {}", destPath);
        return;
      }

      log.info("Copying {} to {}", srcPath, destPath);
      // the following call does not throw an exception if the file/dir does not exist
      fs.deleteRecursively(new Path(destPath));

      input = fs.open(srcPath);
      try {
        decryptingInput = DfsLogger.getDecryptingStream(input, conf);
      } catch (LogHeaderIncompleteException e) {
        log.warn("Could not read header from write-ahead log {}. Not sorting.", srcPath);
        // Creating a 'finished' marker will cause recovery to proceed normally and the
        // empty file will be correctly ignored downstream.
        fs.mkdirs(new Path(destPath));
        writeBuffer(destPath, Collections.emptyList(), part++);
        fs.create(SortedLogState.getFinishedMarkerPath(destPath)).close();
        return;
      }

      final long bufferSize = conf.getAsBytes(Property.TSERV_SORT_BUFFER_SIZE);
      Thread.currentThread().setName("Sorting " + name + " for recovery");
      while (true) {
        final ArrayList<Pair<LogFileKey,LogFileValue>> buffer = new ArrayList<>();
        try {
          long start = input.getPos();
          while (input.getPos() - start < bufferSize) {
            LogFileKey key = new LogFileKey();
            LogFileValue value = new LogFileValue();
            key.readFields(decryptingInput);
            value.readFields(decryptingInput);
            buffer.add(new Pair<>(key, value));
          }
          writeBuffer(destPath, buffer, part++);
          buffer.clear();
        } catch (EOFException ex) {
          writeBuffer(destPath, buffer, part++);
          break;
        }
      }
      fs.create(new Path(destPath, "finished")).close();
      log.info("Finished log sort {} {} bytes {} parts in {}ms", name, getBytesCopied(), part,
          getSortTime());
    }

    synchronized void close() throws IOException {
      // If we receive an empty or malformed-header WAL, we won't
      // have input streams that need closing. Avoid the NPE.
      if (input != null) {
        bytesCopied = input.getPos();
        input.close();
        decryptingInput.close();
        input = null;
      }
    }

    public synchronized long getSortTime() {
      if (sortStart > 0) {
        if (sortStop > 0)
          return sortStop - sortStart;
        return System.currentTimeMillis() - sortStart;
      }
      return 0;
    }

    synchronized long getBytesCopied() throws IOException {
      return input == null ? bytesCopied : input.getPos();
    }
  }

  ThreadPoolExecutor threadPool;
  private final ServerContext context;
  private double walBlockSize;
  private final CryptoService cryptoService;

  public LogSorter(ServerContext context, AccumuloConfiguration conf) {
    this.context = context;
    this.conf = conf;
    int threadPoolSize = conf.getCount(Property.TSERV_RECOVERY_MAX_CONCURRENT);
    this.threadPool =
        ThreadPools.createFixedThreadPool(threadPoolSize, this.getClass().getName(), false);
    this.walBlockSize = DfsLogger.getWalBlockSize(conf);
    this.cryptoService = CryptoServiceFactory.newInstance(conf,
        CryptoServiceFactory.ClassloaderType.ACCUMULO, CryptoEnvironment.Scope.TABLE);
  }

  @VisibleForTesting
  void writeBuffer(String destPath, List<Pair<LogFileKey,LogFileValue>> buffer, int part)
      throws IOException {
    String filename = String.format("part-r-%05d.rf", part);
    Path path = new Path(destPath, filename);
    FileSystem fs = context.getVolumeManager().getFileSystemByPath(path);
    Path fullPath = fs.makeQualified(path);

    // convert the LogFileKeys to Keys, sort and collect the mutations
    Map<Key,List<Mutation>> keyListMap = new TreeMap<>();
    for (Pair<LogFileKey,LogFileValue> pair : buffer) {
      var logFileKey = pair.getFirst();
      var logFileValue = pair.getSecond();
      Key k = logFileKey.toKey();
      var list = keyListMap.putIfAbsent(k, logFileValue.mutations);
      if (list != null) {
        var muts = new ArrayList<>(list);
        muts.addAll(logFileValue.mutations);
        keyListMap.put(logFileKey.toKey(), muts);
      }
    }

    try (var writer = FileOperations.getInstance().newWriterBuilder()
        .forFile(fullPath.toString(), fs, fs.getConf(), cryptoService).withTableConfiguration(conf)
        .build()) {
      writer.startDefaultLocalityGroup();
      for (var entry : keyListMap.entrySet()) {
        LogFileValue val = new LogFileValue();
        val.mutations = entry.getValue();
        writer.append(entry.getKey(), val.toValue());
      }
    }
  }

  public void startWatchingForRecoveryLogs(ThreadPoolExecutor distWorkQThreadPool)
      throws KeeperException, InterruptedException {
    this.threadPool = distWorkQThreadPool;
    new DistributedWorkQueue(context.getZooKeeperRoot() + Constants.ZRECOVERY, conf)
        .startProcessing(new LogProcessor(), this.threadPool);
  }

  public List<RecoveryStatus> getLogSorts() {
    List<RecoveryStatus> result = new ArrayList<>();
    synchronized (currentWork) {
      for (Entry<String,LogProcessor> entries : currentWork.entrySet()) {
        RecoveryStatus status = new RecoveryStatus();
        status.name = entries.getKey();
        try {
          double progress = entries.getValue().getBytesCopied() / walBlockSize;
          // to be sure progress does not exceed 100%
          status.progress = Math.min(progress, 99.9);
        } catch (IOException ex) {
          log.warn("Error getting bytes read");
        }
        status.runtime = (int) entries.getValue().getSortTime();
        result.add(status);
      }
      return result;
    }
  }
}
