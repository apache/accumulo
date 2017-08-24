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
package org.apache.accumulo.tserver.log;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue.Processor;
import org.apache.accumulo.tserver.log.DfsLogger.DFSLoggerInputStreams;
import org.apache.accumulo.tserver.log.DfsLogger.LogHeaderIncompleteException;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class LogSorter {

  private static final Logger log = LoggerFactory.getLogger(LogSorter.class);
  VolumeManager fs;
  AccumuloConfiguration conf;

  private final Map<String,LogProcessor> currentWork = Collections.synchronizedMap(new HashMap<String,LogProcessor>());

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

      try {
        log.info("Copying {} to {}", src, dest);
        sort(sortId, new Path(src), dest);
      } finally {
        currentWork.remove(sortId);
      }

    }

    public void sort(String name, Path srcPath, String destPath) {

      synchronized (this) {
        sortStart = System.currentTimeMillis();
      }

      String formerThreadName = Thread.currentThread().getName();
      int part = 0;
      try {

        // the following call does not throw an exception if the file/dir does not exist
        fs.deleteRecursively(new Path(destPath));

        DFSLoggerInputStreams inputStreams;
        try {
          inputStreams = DfsLogger.readHeaderAndReturnStream(fs, srcPath, conf);
        } catch (LogHeaderIncompleteException e) {
          log.warn("Could not read header from write-ahead log {}. Not sorting.", srcPath);
          // Creating a 'finished' marker will cause recovery to proceed normally and the
          // empty file will be correctly ignored downstream.
          fs.mkdirs(new Path(destPath));
          writeBuffer(destPath, Collections.<Pair<LogFileKey,LogFileValue>> emptyList(), part++);
          fs.create(SortedLogState.getFinishedMarkerPath(destPath)).close();
          return;
        }

        this.input = inputStreams.getOriginalInput();
        this.decryptingInput = inputStreams.getDecryptingInputStream();

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
        log.info("Finished log sort {} {} bytes {} parts in {}ms", name, getBytesCopied(), part, getSortTime());
      } catch (Throwable t) {
        try {
          // parent dir may not exist
          fs.mkdirs(new Path(destPath));
          fs.create(SortedLogState.getFailedMarkerPath(destPath)).close();
        } catch (IOException e) {
          log.error("Error creating failed flag file " + name, e);
        }
        log.error("Caught throwable", t);
      } finally {
        Thread.currentThread().setName(formerThreadName);
        try {
          close();
        } catch (Exception e) {
          log.error("Error during cleanup sort/copy " + name, e);
        }
        synchronized (this) {
          sortStop = System.currentTimeMillis();
        }
      }
    }

    private void writeBuffer(String destPath, List<Pair<LogFileKey,LogFileValue>> buffer, int part) throws IOException {
      Path path = new Path(destPath, String.format("part-r-%05d", part));
      FileSystem ns = fs.getVolumeByPath(path).getFileSystem();

      MapFile.Writer output = new MapFile.Writer(ns.getConf(), ns.makeQualified(path), MapFile.Writer.keyClass(LogFileKey.class),
          MapFile.Writer.valueClass(LogFileValue.class));
      try {
        Collections.sort(buffer, new Comparator<Pair<LogFileKey,LogFileValue>>() {
          @Override
          public int compare(Pair<LogFileKey,LogFileValue> o1, Pair<LogFileKey,LogFileValue> o2) {
            return o1.getFirst().compareTo(o2.getFirst());
          }
        });
        for (Pair<LogFileKey,LogFileValue> entry : buffer) {
          output.append(entry.getFirst(), entry.getSecond());
        }
      } finally {
        output.close();
      }
    }

    synchronized void close() throws IOException {
      // If we receive an empty or malformed-header WAL, we won't
      // have input streams that need closing. Avoid the NPE.
      if (null != input) {
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
  private final Instance instance;

  public LogSorter(Instance instance, VolumeManager fs, AccumuloConfiguration conf) {
    this.instance = instance;
    this.fs = fs;
    this.conf = conf;
    int threadPoolSize = conf.getCount(Property.TSERV_RECOVERY_MAX_CONCURRENT);
    this.threadPool = new SimpleThreadPool(threadPoolSize, this.getClass().getName());
  }

  public void startWatchingForRecoveryLogs(ThreadPoolExecutor distWorkQThreadPool) throws KeeperException, InterruptedException {
    this.threadPool = distWorkQThreadPool;
    new DistributedWorkQueue(ZooUtil.getRoot(instance) + Constants.ZRECOVERY, conf).startProcessing(new LogProcessor(), this.threadPool);
  }

  public List<RecoveryStatus> getLogSorts() {
    List<RecoveryStatus> result = new ArrayList<>();
    synchronized (currentWork) {
      for (Entry<String,LogProcessor> entries : currentWork.entrySet()) {
        RecoveryStatus status = new RecoveryStatus();
        status.name = entries.getKey();
        try {
          status.progress = entries.getValue().getBytesCopied() / (0.0 + conf.getAsBytes(Property.TSERV_WALOG_MAX_SIZE));
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
