/**
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
package org.apache.accumulo.server.tabletserver.log;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.logger.LogFileKey;
import org.apache.accumulo.server.logger.LogFileValue;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.server.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * 
 */
public class LogSorter {
  
  private static final Logger log = Logger.getLogger(LogSorter.class);
  FileSystem fs;
  AccumuloConfiguration conf;
  
  private Map<String,Work> currentWork = new HashMap<String,Work>();

  class Work implements Runnable {
    final String name;
    FSDataInputStream input;
    final String destPath;
    long bytesCopied = -1;
    long sortStart = 0;
    long sortStop = -1;
    private final LogSortNotifier cback;
    
    synchronized long getBytesCopied() throws IOException {
      return input == null ? bytesCopied : input.getPos();
    }
    
    Work(String name, FSDataInputStream input, String destPath, LogSortNotifier cback) {
      this.name = name;
      this.input = input;
      this.destPath = destPath;
      this.cback = cback;
    }
    synchronized boolean finished() {
      return input == null;
    }
    public void run() {
      sortStart = System.currentTimeMillis();
      String formerThreadName = Thread.currentThread().getName();
      int part = 0;
      try {
        final long bufferSize = conf.getMemoryInBytes(Property.TSERV_SORT_BUFFER_SIZE);
        Thread.currentThread().setName("Sorting " + name + " for recovery");
        while (true) {
          final ArrayList<Pair<LogFileKey, LogFileValue>> buffer = new ArrayList<Pair<LogFileKey, LogFileValue>>();
          try {
            long start = input.getPos();
            while (input.getPos() - start < bufferSize) {
              LogFileKey key = new LogFileKey();
              LogFileValue value = new LogFileValue();
              key.readFields(input);
              value.readFields(input);
              buffer.add(new Pair<LogFileKey, LogFileValue>(key, value));
            }
            writeBuffer(buffer, part++);
            buffer.clear();
          } catch (EOFException ex) {
            writeBuffer(buffer, part++);
            break;
          }
        }
        fs.create(new Path(destPath, "finished")).close();
        log.info("Log copy/sort of " + name + " complete");
      } catch (Throwable t) {
        try {
          fs.create(new Path(destPath, "failed")).close();
        } catch (IOException e) {
          log.error("Error creating failed flag file " + name, e);
        }
        log.error(t, t);
        try {
          cback.notice(name, getBytesCopied(), part, getSortTime(), t.toString());
        } catch (Exception ex) {
          log.error("Strange error notifying the master of a logSort problem for file " + name);
        }
      } finally {
        Thread.currentThread().setName(formerThreadName);
        try {
          close();
        } catch (IOException e) {
          log.error("Error during cleanup sort/copy " + name, e);
        }
        sortStop = System.currentTimeMillis();
        synchronized (currentWork) {
          currentWork.remove(name);
        }
        try {
          cback.notice(name, getBytesCopied(), part, getSortTime(), "");
        } catch (Exception ex) {
          log.error("Strange error reporting successful log sort " + name, ex);
        }
      }
    }
    
    private void writeBuffer(ArrayList<Pair<LogFileKey,LogFileValue>> buffer, int part) throws IOException {
      String path = destPath + String.format("/part-r-%05d", part++);
      MapFile.Writer output = new MapFile.Writer(fs.getConf(), fs, path, LogFileKey.class, LogFileValue.class);
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
      bytesCopied = input.getPos();
      input.close();
      input = null;
    }
    
    public synchronized long getSortTime() {
      if (sortStart > 0) {
        if (sortStop > 0)
          return sortStop - sortStart;
        return System.currentTimeMillis() - sortStart;
      }
      return 0;
    }
  };
  
  final ThreadPoolExecutor threadPool;
  private Instance instance;
  
  public LogSorter(Instance instance, FileSystem fs, AccumuloConfiguration conf) {
    this.instance = instance;
    this.fs = fs;
    this.conf = conf;
    int threadPoolSize = conf.getCount(Property.TSERV_RECOVERY_MAX_CONCURRENT);
    this.threadPool = new SimpleThreadPool(threadPoolSize, this.getClass().getName());
  }
  
  public void startWatchingForRecoveryLogs(final String serverName) throws KeeperException, InterruptedException {
    final String path = ZooUtil.getRoot(instance) + Constants.ZRECOVERY;
    final ZooReaderWriter zoo = ZooReaderWriter.getInstance();
    zoo.mkdirs(path);
    List<String> children = zoo.getChildren(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        switch (event.getType()) {
          case NodeChildrenChanged:
            if (event.getPath().equals(path))
              try {
                attemptRecoveries(zoo, serverName, path, zoo.getChildren(path));
              } catch (KeeperException e) {
                log.error("Unable to get recovery information", e);
              } catch (InterruptedException e) {
                log.info("Interrupted getting recovery information", e);
              }
            else
              log.info("Unexpected path for NodeChildrenChanged event " + event.getPath());
            break;
          case NodeCreated:
          case NodeDataChanged:
          case NodeDeleted:
          case None:
            log.info("Got unexpected zookeeper event: " + event.getType() + " for " + path);
            break;
          
        }
      }
    });
    attemptRecoveries(zoo, serverName, path, children);
    Random r = new Random();
    // Add a little jitter to avoid all the tservers slamming zookeeper at once
    SimpleTimer.getInstance().schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          attemptRecoveries(zoo, serverName, path, zoo.getChildren(path));
        } catch (KeeperException e) {
          log.error("Unable to get recovery information", e);
        } catch (InterruptedException e) {
          log.info("Interrupted getting recovery information", e);
        }        
      }
    }, r.nextInt(1000), 60 * 1000);
  }
  
  private void attemptRecoveries(final ZooReaderWriter zoo, final String serverName, String path, List<String> children) {
    if (children.size() == 0)
      return;
    log.info("Zookeeper references " + children.size() + " recoveries, attempting locks");
    Random random = new Random();
    Collections.shuffle(children, random);
    try {
      for (String child : children) {
        final String childPath = path + "/" + child;
        log.debug("Attempting to lock " + child);
        ZooLock lock = new ZooLock(childPath);
        if (lock.tryLock(new LockWatcher() {
          @Override
          public void lostLock(LockLossReason reason) {
            log.info("Ignoring lost lock event, reason " + reason);
          }
        }, serverName.getBytes())) {
          // Great... we got the lock, but maybe we're too busy
          if (threadPool.getQueue().size() > 1) {
            lock.unlock();
            log.debug("got the lock, but thread pool is busy; released the lock on " + child);
            continue;
          }
          log.info("got lock for " + child);
          byte[] contents = zoo.getData(childPath, null);
          String destination = Constants.getRecoveryDir(conf) + "/" + child;
          startSort(new String(contents), destination, new LogSortNotifier() {
            @Override
            public void notice(String name, long bytes, int parts, long milliseconds, String error) {
              log.info("Finished log sort " + name + " " + bytes + " bytes " + parts + " parts in " + milliseconds + "ms");
              try {
                zoo.recursiveDelete(childPath, NodeMissingPolicy.SKIP);
              } catch (Exception e) {
                log.error("Error received when trying to delete recovery entry in zookeeper " + childPath);
              }
            }
          });
        } else {
          log.info("failed to get the lock " + child);
        }
      }
    } catch (Throwable t) {
      log.error("Unexpected error", t);
    }
  }

  public interface LogSortNotifier {
    public void notice(String name, long bytes, int parts, long milliseconds, String error);
  }

  private void startSort(String src, String dest, LogSortNotifier cback) throws IOException {
    log.info("Copying " + src + " to " + dest);
    fs.delete(new Path(dest), true);
    Path srcPath = new Path(src);
    synchronized (currentWork) {
      Work work = new Work(srcPath.getName(), fs.open(srcPath), dest, cback);
      if (!currentWork.containsKey(srcPath.getName())) {
        threadPool.execute(work);
        currentWork.put(srcPath.getName(), work);
      }
    }
  }
  
  public List<RecoveryStatus> getLogSorts() {
    List<RecoveryStatus> result = new ArrayList<RecoveryStatus>();
    synchronized (currentWork) {
      for (Entry<String,Work> entries : currentWork.entrySet()) {
        RecoveryStatus status = new RecoveryStatus();
        status.name = entries.getKey();
        try {
          status.progress = entries.getValue().getBytesCopied() / (0.0 + conf.getMemoryInBytes(Property.TSERV_WALOG_MAX_SIZE));
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
