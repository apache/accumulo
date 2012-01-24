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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
  private final Set<String> archiving = Collections.synchronizedSet(new HashSet<String>());
  
  private FileSystem src;
  private FileSystem dest;
  private String destDir;
  private final boolean archive;
  private short replication;
  
  static Path archiveName(String fullPath) {
    if (isArchive(fullPath))
      return new Path(fullPath);
    return new Path(fullPath + ".archiving");
  }
  
  static public boolean isArchive(String fullPath) {
    return fullPath.endsWith(".archiving");
  }
  
  static public String origName(String archiving) {
    if (!isArchive(archiving))
      throw new IllegalArgumentException(archiving);
    return archiving.substring(0, archiving.length() - ".archiving".length());
  }
  
  class LogArchiveTask implements Runnable {
    
    private FileSystem src;
    private FileSystem dest;
    private String fullPath;
    private String destDir;
    
    public LogArchiveTask(FileSystem src, FileSystem dest, String fullPath, String destDir) {
      this.src = src;
      this.dest = dest;
      this.fullPath = fullPath;
      this.destDir = destDir;
    }
    
    @Override
    public void run() {
      Path srcPath = archiveName(fullPath);
      String name = srcPath.getName();
      try {
        dest.delete(new Path(destDir, name + ".gz_tmp"), false);
        
        FSDataInputStream in = src.open(srcPath);
        GZIPOutputStream out = new GZIPOutputStream(dest.create(new Path(destDir, name + ".gz_tmp"), false, dest.getConf().getInt("io.file.buffer.size", 4096),
            replication == 0 ? dest.getDefaultReplication() : replication, dest.getDefaultBlockSize()));
        
        byte buf[] = new byte[1 << 20];
        int numRead;
        
        while ((numRead = in.read(buf)) != -1) {
          out.write(buf, 0, numRead);
        }
        
        in.close();
        out.close();
        
        dest.rename(new Path(destDir, name + ".gz_tmp"), new Path(destDir, name + ".gz"));
        src.delete(srcPath, true);
        
      } catch (Exception e) {
        log.error("Failed to archive " + fullPath, e);
      } finally {
        archiving.remove(name);
      }
    }
    
  }
  
  LogArchiver(AccumuloConfiguration conf, FileSystem src, FileSystem dest, List<String> logDirs, boolean archive) throws IOException {
    this.src = src;
    this.dest = dest;
    this.threadPool = Executors.newSingleThreadExecutor();
    this.destDir = conf.get(Property.INSTANCE_DFS_DIR) + "/walogArchive";
    this.archive = archive;
    this.replication = (short) conf.getCount(Property.LOGGER_ARCHIVE_REPLICATION);
    dest.mkdirs(new Path(destDir));
    
    if (archive) {
      // re-start any previously started archives
      for (String logDir : logDirs) {
        try {
          FileStatus files[] = src.listStatus(new Path(logDir));
          if (files != null) {
            for (FileStatus file : files) {
              String name = file.getPath().getName();
              if (isArchive(name)) {
                log.info("archiving " + origName(name));
                threadPool.execute(new LogArchiveTask(src, dest, file.getPath().toString(), destDir));
              }
            }
          }
        } catch (IOException e) {
          log.warn("Failed to process existing files in local archive dir " + logDir, e);
        }
      }
    }
  }
  
  public void archive(final String fullName) throws IOException {
    final Path fullPath = new Path(fullName);
    final String name = fullPath.getName();
    if (archiving.contains(name))
      return;
    archiving.add(name);
    
    if (!archive) {
      threadPool.execute(new Runnable() {
        @Override
        public void run() {
          try {
            if (src.delete(fullPath, true))
              log.info(fullPath + " deleted");
            else
              log.error("Unable to delete " + fullPath);
          } catch (Exception ex) {
            log.error("Error trying to delete " + fullPath + ": " + ex);
          } finally {
            archiving.remove(name);
          }
        }
        
      });
    } else {
      log.info("archiving " + name);
      src.rename(fullPath, archiveName(fullName));
      threadPool.execute(new LogArchiveTask(src, dest, fullName, destDir));
    }
  }
}
