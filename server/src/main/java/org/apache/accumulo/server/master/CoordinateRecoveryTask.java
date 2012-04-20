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
package org.apache.accumulo.server.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.tabletserver.thrift.LogCopyInfo;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.tabletserver.log.RemoteLogger;
import org.apache.accumulo.server.trace.TraceFileSystem;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class CoordinateRecoveryTask implements Runnable {
  private static final Logger log = Logger.getLogger(CoordinateRecoveryTask.class);
  
  private FileSystem fs;
  private Map<String,RecoveryJob> processing = new HashMap<String,RecoveryJob>();
  
  private boolean stop = false;
  
  private ZooCache zcache;
  
  private static String fullName(String name) {
    return ServerConstants.getRecoveryDir() + "/" + name;
  }
  
  static class LogFile {
    final String server;
    final String file;
    
    LogFile(String metadataEntry) {
      String parts[] = metadataEntry.split("/");
      if (parts.length != 2)
        throw new RuntimeException("Bad log file name: " + metadataEntry);
      server = parts[0];
      file = parts[1];
    }
    
    public String toString() {
      return server + "/" + file;
    }
    
    String recoveryFileName() {
      return fullName(file + ".recovered");
    }
    
    String successFileName() {
      return fullName(file + ".recovered/finished");
    }
    
    String failedFileName() {
      return fullName(file + ".failed");
    }
    
    public String unsortedFileName() {
      return fullName(file);
    }
    
    public String copyTempFileName() {
      return fullName(file + ".copy");
    }
    
  }
  
  interface JobComplete {
    void finished(LogFile entry);
  }
  
  /**
   * Track a log file through two asynchronous steps:
   * <ul>
   * <li>copy to HDFS
   * <li>sort via map/reduce
   * <ul>
   */
  private class RecoveryJob {
    final LogFile logFile;
    final long copyStartTime;
    long copySize = 0;
    JobComplete notify = null;
    final AccumuloConfiguration config;
    String loggerZNode;
    
    RecoveryJob(LogFile entry, JobComplete callback, AccumuloConfiguration conf) throws Exception {
      logFile = entry;
      copyStartTime = System.currentTimeMillis();
      notify = callback;
      config = conf;
    }
    
    private void startCopy() throws Exception {
      log.debug("Starting log recovery: " + logFile);
      try {
        // Ask the logging server to put the file in HDFS
        RemoteLogger logger = new RemoteLogger(logFile.server, config);
        String base = logFile.unsortedFileName();
        log.debug("Starting to copy " + logFile.file + " from " + logFile.server);
        LogCopyInfo lci = logger.startCopy(logFile.file, base);
        copySize = lci.fileSize;
        loggerZNode = lci.loggerZNode;
      } catch (Throwable t) {
        log.warn("Unable to recover " + logFile + "(" + t + ")", t);
        fail();
      }
      
    }
    
    synchronized boolean isComplete() throws Exception {
      if (fs.exists(new Path(logFile.successFileName()))) {
        return true;
      }
      if (fs.exists(new Path(logFile.failedFileName()))) {
        return true;
      }
      
      if (zcache.get(loggerZNode) == null) {
        log.debug("zknode " + loggerZNode + " is gone, copy " + logFile.file + " from " + logFile.server + " assumed dead");
        return true;
      }

      if (elapsedMillis() > config.getTimeInMillis(Property.MASTER_RECOVERY_MAXTIME)) {
        log.warn("Recovery taking too long, giving up");
        return true;
      }
      
      // Did the sort fail?
      if (fs.exists(new Path(logFile.failedFileName()))) {
        return true;
      }
      
      log.debug(toString());
      return false;
    }
    
    private long elapsedMillis() {
      return (System.currentTimeMillis() - this.copyStartTime);
    }
    
    synchronized void fail(boolean createFailFlag) {
      String failed = logFile.failedFileName();
      try {
        if (createFailFlag)
          fs.create(new Path(failed)).close();
      } catch (IOException e) {
        log.warn("Unable to create recovery fail marker" + failed);
      }
      log.warn("Recovery of " + logFile.server + ":" + logFile.file + " failed");
    }
    
    synchronized void fail() {
      fail(true);
    }
    
    synchronized public String toString() {
      return String.format("Copying %s from %s (for %f seconds) %2.1f", logFile.file, logFile.server, elapsedMillis() / 1000., copiedSoFar() * 100. / copySize);
    }
    
    synchronized long copiedSoFar() {
      try {
        ContentSummary contentSummary = fs.getContentSummary(new Path(logFile.recoveryFileName()));
        // map files are bigger than sequence files
        return (long) (contentSummary.getSpaceConsumed() * .8);
      } catch (Exception ex) {
        return 0;
      }
    }
    
    synchronized public RecoveryStatus getStatus() throws IOException {
      try {
        return new RecoveryStatus(logFile.server, logFile.file, 0., 0., (int) (System.currentTimeMillis() - copyStartTime), (copiedSoFar() / (double) copySize));
      } catch (Exception e) {
        return new RecoveryStatus(logFile.server, logFile.file, 1.0, 1.0, (int) (System.currentTimeMillis() - copyStartTime), 1.0);
      }
    }
  }
  
  AccumuloConfiguration config;
  public CoordinateRecoveryTask(FileSystem fs, AccumuloConfiguration conf) {
    this.fs = fs;
    this.config = conf;
    zcache = new ZooCache();
  }
  
  public boolean recover(AuthInfo credentials, KeyExtent extent, Collection<Collection<String>> entries, JobComplete notify) {
    boolean finished = true;
    log.debug("Log entries: " + entries);
    for (Collection<String> set : entries) {
      // if any file from the log set exists, use that:
      boolean found = false;
      for (String metadataEntry : set) {
        LogFile logFile = new LogFile(metadataEntry);
        String recovered = logFile.successFileName();
        try {
          if (fs.exists(new Path(recovered))) {
            log.debug("Found recovery file " + recovered);
            found = true;
            break;
          }
        } catch (IOException ex) {
          log.info("Error looking for recovery files", ex);
        }
      }
      if (found)
        continue;
      finished = false;
      // Start recovering all the logs we could need
      for (String metadataEntry : set) {
        LogFile logFile = new LogFile(metadataEntry);
        String failed = logFile.failedFileName();
        String recovered = logFile.recoveryFileName();
        RecoveryJob job = null;
        try {
          synchronized (processing) {
            if (!fs.exists(new Path(failed)) && !fs.exists(new Path(recovered)) && !processing.containsKey(metadataEntry)) {
              processing.put(metadataEntry, job = new RecoveryJob(logFile, notify, config));
            }
          }
          if (job != null) {
            job.startCopy();
          }
        } catch (Exception ex) {
          log.warn("exception starting recovery " + ex);
        }
      }
    }
    return finished;
  }
  
  @Override
  public void run() {
    // Check on the asynchronous requests: keep them moving along
    int count = 0;
    while (!stop) {
      try {
        synchronized (processing) {
          List<Entry<String,RecoveryJob>> entries = new ArrayList<Entry<String,RecoveryJob>>(processing.entrySet());
          for (Entry<String,RecoveryJob> job : entries) {
            try {
              if (job.getValue().isComplete()) {
                processing.remove(job.getKey());
                processing.notifyAll();
                job.getValue().notify.finished(job.getValue().logFile);
              }
            } catch (Throwable t) {
              log.error("Error checking on job", t);
              processing.remove(job.getKey());
              job.getValue().fail();
              processing.notifyAll();
              job.getValue().notify.finished(job.getValue().logFile);
            }
          }
        }
        // every now and then, clean up old files
        if (count++ % 10 == 0) {
          removeOldRecoverFiles();
        }
        UtilWaitThread.sleep(1000);
      } catch (Throwable t) {
        log.error("Unexpected exception caught", t);
      }
    }
  }
  
  private void removeOldRecoverFiles() throws IOException {
    long now = System.currentTimeMillis();
    long maxAgeInMillis = config.getTimeInMillis(Property.MASTER_RECOVERY_MAXAGE);
    FileStatus[] children = fs.listStatus(new Path(ServerConstants.getRecoveryDir()));
    if (children != null) {
      for (FileStatus child : children) {
        if (now - child.getModificationTime() > maxAgeInMillis && !fs.delete(child.getPath(), true)) {
          log.warn("Unable to delete old recovery directory: " + child.getPath());
        }
      }
    }
  }
  
  public List<RecoveryStatus> status() {
    List<RecoveryStatus> result = new ArrayList<RecoveryStatus>();
    synchronized (processing) {
      for (RecoveryJob job : processing.values()) {
        try {
          result.add(job.getStatus());
        } catch (IOException ex) {
          log.warn("Ignoring error getting job status");
        }
      }
    }
    return result;
  }
  
  public synchronized void stop() {
    stop = true;
  }
}
