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
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.tabletserver.log.RemoteLogger;
import org.apache.accumulo.server.trace.TraceFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

public class CoordinateRecoveryTask implements Runnable {
  private static final Logger log = Logger.getLogger(CoordinateRecoveryTask.class);
  
  private FileSystem fs;
  private Map<String,RecoveryJob> processing = new HashMap<String,RecoveryJob>();
  
  private boolean stop = false;
  
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
    Job sortJob = null;
    boolean useMapReduce = ServerConfiguration.getSystemConfiguration().getBoolean(Property.MASTER_RECOVERY_SORT_MAPREDUCE);
    JobComplete notify = null;
    
    RecoveryJob(LogFile entry, JobComplete callback) throws Exception {
      logFile = entry;
      copyStartTime = System.currentTimeMillis();
      notify = callback;
    }
    
    private void startCopy() throws Exception {
      log.debug("Starting log recovery: " + logFile);
      try {
        // Ask the logging server to put the file in HDFS
        RemoteLogger logger = new RemoteLogger(logFile.server);
        String base = logFile.unsortedFileName();
        log.debug("Starting to copy " + logFile.file + " from " + logFile.server);
        copySize = logger.startCopy(logFile.file, base, !useMapReduce);
      } catch (Throwable t) {
        log.warn("Unable to recover " + logFile + "(" + t + ")", t);
        fail();
      }
      
    }
    
    synchronized private void startSort() throws Exception {
      Integer reducers = ServerConfiguration.getSystemConfiguration().getCount(Property.MASTER_RECOVERY_REDUCERS);
      String queue = ServerConfiguration.getSystemConfiguration().get(Property.MASTER_RECOVERY_QUEUE);
      String pool = ServerConfiguration.getSystemConfiguration().get(Property.MASTER_RECOVERY_POOL);
      String result = logFile.recoveryFileName();
      fs.delete(new Path(result), true);
      List<String> jars = new ArrayList<String>();
      jars.addAll(jarsLike("accumulo-core"));
      jars.addAll(jarsLike("zookeeper"));
      jars.addAll(jarsLike("libthrift"));
      sortJob = LogSort.startSort(true,
          new String[] {"-libjars", StringUtil.join(jars, ","), "-r", reducers.toString(), "-q", queue, "-p", pool, logFile.unsortedFileName(), result});
    }
    
    synchronized boolean isComplete() throws Exception {
      if (fs.exists(new Path(logFile.successFileName()))) {
        return true;
      }
      if (fs.exists(new Path(logFile.failedFileName()))) {
        return true;
      }
      
      if (elapsedMillis() > ServerConfiguration.getSystemConfiguration().getTimeInMillis(Property.MASTER_RECOVERY_MAXTIME)) {
        log.warn("Recovery taking too long, giving up");
        if (sortJob != null)
          sortJob.killJob();
        return true;
      }
      
      // Did the sort fail?
      if (sortJob == null && fs.exists(new Path(logFile.failedFileName()))) {
        return true;
      }
      
      log.debug(toString());
      
      // Did the copy complete?
      if (useMapReduce && fs.exists(new Path(logFile.unsortedFileName()))) {
        // Start the sort or check on it
        if (sortJob == null) {
          log.debug("Finished copy of " + logFile.file + " from " + logFile.server + ": took " + (elapsedMillis() / 1000.) + " seconds, starting sort");
          startSort();
        }
      }
      return false;
    }
    
    private long elapsedMillis() {
      return (System.currentTimeMillis() - this.copyStartTime);
    }
    
    synchronized void fail(boolean createFailFlag) {
      sortJob = null;
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
      if (sortJob != null) {
        try {
          return String.format("Sorting log %s job %s: %2.1f/%2.1f", logFile.file, sortJob.getTrackingURL(), sortJob.mapProgress() * 100,
              sortJob.reduceProgress() * 100);
        } catch (Exception e) {
          log.debug("Unable to get stats for sort of " + logFile.file, e);
        }
      }
      return String.format("Copying %s from %s (for %f seconds) %2.1f", logFile.file, logFile.server, elapsedMillis() / 1000., copiedSoFar() * 100. / copySize);
    }
    
    synchronized long copiedSoFar() {
      try {
        if (useMapReduce) {
          Path unsorted = new Path(logFile.unsortedFileName());
          if (fs.exists(unsorted))
            return fs.getFileStatus(unsorted).getLen();
          return fs.getFileStatus(new Path(logFile.copyTempFileName())).getLen();
        } else {
          ContentSummary contentSummary = fs.getContentSummary(new Path(logFile.recoveryFileName()));
          // map files are bigger than sequence files
          return (long) (contentSummary.getSpaceConsumed() * .8);
        }
      } catch (Exception ex) {
        return 0;
      }
    }
    
    synchronized public RecoveryStatus getStatus() throws IOException {
      try {
        return new RecoveryStatus(logFile.server, logFile.file, (sortJob == null ? 0. : sortJob.mapProgress()), (sortJob == null ? 0.
            : sortJob.reduceProgress()), (int) (System.currentTimeMillis() - copyStartTime), (sortJob != null) ? 1. : (copySize == 0 ? 0 : copiedSoFar()
            / (double) copySize));
      } catch (NullPointerException npe) {
        return new RecoveryStatus(logFile.server, logFile.file, 1.0, 1.0, (int) (System.currentTimeMillis() - copyStartTime), 1.0);
      }
    }
  }
  
  public CoordinateRecoveryTask(FileSystem fs) {
    this.fs = fs;
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
              processing.put(metadataEntry, job = new RecoveryJob(logFile, notify));
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
  
  private static List<String> jarsLike(String substr) {
    ArrayList<String> result = new ArrayList<String>();
    URLClassLoader loader = (URLClassLoader) CoordinateRecoveryTask.class.getClassLoader();
    for (URL url : loader.getURLs()) {
      String path = url.getPath();
      if (path.indexOf(substr) >= 0 && path.endsWith(".jar") && path.indexOf("javadoc") < 0 && path.indexOf("sources") < 0) {
        result.add(path);
      }
    }
    return result;
  }
  
  void cleanupOldJobs() {
    try {
      Configuration conf = CachedConfiguration.getInstance();
      @SuppressWarnings("deprecation")
      JobClient jc = new JobClient(new org.apache.hadoop.mapred.JobConf(conf));
      for (JobStatus status : jc.getAllJobs()) {
        if (!status.isJobComplete()) {
          RunningJob job = jc.getJob(status.getJobID());
          if (job.getJobName().equals(LogSort.getJobName())) {
            log.info("found a running " + job.getJobName());
            Configuration jobConfig = new Configuration(false);
            log.info("fetching configuration from " + job.getJobFile());
            jobConfig.addResource(TraceFileSystem.wrap(FileUtil.getFileSystem(conf, ServerConfiguration.getSiteConfiguration())).open(
                new Path(job.getJobFile())));
            if (HdfsZooInstance.getInstance().getInstanceID().equals(jobConfig.get(LogSort.INSTANCE_ID_PROPERTY))) {
              log.info("Killing job " + job.getID().toString());
            }
          }
        }
      }
      FileStatus[] children = fs.listStatus(new Path(ServerConstants.getRecoveryDir()));
      if (children != null) {
        for (FileStatus child : children) {
          log.info("Deleting recovery directory " + child);
          fs.delete(child.getPath(), true);
        }
      }
    } catch (IOException e) {
      log.error("Error cleaning up old Log Sort jobs" + e);
    } catch (Exception e) {
      log.error("Unknown error cleaning up old jobs", e);
    }
  }
  
  @Override
  public void run() {
    // Check on the asynchronous requests: keep them moving along
    int count = 0;
    cleanupOldJobs();
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
    long maxAgeInMillis = ServerConfiguration.getSystemConfiguration().getTimeInMillis(Property.MASTER_RECOVERY_MAXAGE);
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
