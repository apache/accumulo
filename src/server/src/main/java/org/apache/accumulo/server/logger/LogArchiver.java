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
package org.apache.accumulo.server.logger;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LogArchiver {
  
  static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(LogArchiver.class);
  
  private ExecutorService threadPool;
  private String workDir;
  private String logDir;
  
  private String destDir;
  
  private FileSystem src;
  
  private FileSystem dest;
  
  class LogArchiveTask implements Runnable {
    
    private FileSystem src;
    private FileSystem dest;
    private String srcDir;
    private String destDir;
    private String name;
    
    public LogArchiveTask(FileSystem src, FileSystem dest, String srcDir, String destDir, String name) {
      this.src = src;
      this.dest = dest;
      this.srcDir = srcDir;
      this.destDir = destDir;
      this.name = name;
    }
    
    @Override
    public void run() {
      try {
        
        dest.delete(new Path(destDir, name + ".gz_tmp"), false);
        
        FSDataInputStream in = src.open(new Path(srcDir, name));
        GZIPOutputStream out = new GZIPOutputStream(dest.create(new Path(destDir, name + ".gz_tmp"), false));
        
        byte buf[] = new byte[1 << 20];
        int numRead;
        
        while ((numRead = in.read(buf)) != -1) {
          out.write(buf, 0, numRead);
        }
        
        in.close();
        out.close();
        
        dest.rename(new Path(destDir, name + ".gz_tmp"), new Path(destDir, name + ".gz"));
        src.delete(new Path(srcDir, name), true);
        
      } catch (Exception e) {
        log.error("Failed to archive " + name, e);
      }
    }
    
  }
  
  LogArchiver(FileSystem src, FileSystem dest) {
    boolean archive = AccumuloConfiguration.getSystemConfiguration().getBoolean(Property.LOGGER_ARCHIVE);
    this.logDir = AccumuloConfiguration.getSystemConfiguration().get(Property.LOGGER_DIR);
    this.src = src;
    this.dest = dest;
    
    if (archive) {
      this.workDir = logDir + "/archive";
      this.destDir = AccumuloConfiguration.getSystemConfiguration().get(Property.INSTANCE_DFS_DIR) + "/walogArchive";
      threadPool = Executors.newSingleThreadExecutor();
      
      try {
        FileStatus files[] = src.listStatus(new Path(workDir));
        if (files != null) {
          for (FileStatus fileStatus : files) {
            String name = fileStatus.getPath().getName();
            dest.mkdirs(new Path(destDir));
            log.info("archiving " + name);
            threadPool.execute(new LogArchiveTask(src, dest, workDir, destDir, name));
          }
        }
        
      } catch (IOException e) {
        log.warn("Failed to process exiting files in local archive dir " + workDir, e);
      }
      
    } else {
      this.workDir = null;
    }
    
  }
  
  public void archive(String name) throws IOException {
    if (workDir == null || workDir.equals("")) {
      src.delete(new Path(logDir, name), true);
      log.info(name + " deleted");
    } else {
      log.info("archiving " + name);
      src.mkdirs(new Path(workDir));
      dest.mkdirs(new Path(destDir));
      src.rename(new Path(logDir, name), new Path(workDir, name));
      threadPool.execute(new LogArchiveTask(src, dest, workDir, destDir, name));
    }
  }
}
