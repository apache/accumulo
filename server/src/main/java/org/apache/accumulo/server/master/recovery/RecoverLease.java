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
package org.apache.accumulo.server.master.recovery;

import java.io.IOException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.fate.Repo;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.master.tableOps.MasterRepo;
import org.apache.accumulo.server.trace.TraceFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;


public class RecoverLease extends MasterRepo {
  
  private static final long serialVersionUID = 1L;

  private String server;
  private String file;
  private long start;

  public RecoverLease(String server, String file) {
    this.server = server;
    this.file = file;
    this.start = System.currentTimeMillis();
  }
  
  public static Path getSource(Master master, String server, String file) {
    String source = Constants.getWalDirectory(master.getSystemConfiguration()) + "/" + server + "/" + file;
    if (server.contains(":")) {
      // old-style logger log, copied from local file systems by tservers, unsorted into the wal base dir
      source = Constants.getWalDirectory(master.getSystemConfiguration()) + "/" + file;
    }
    return new Path(source);
  }
  
  public Path getSource(Master master) {
    return getSource(master, server, file);
  }

  @Override
  public long isReady(long tid, Master master) throws Exception {
    master.updateRecoveryInProgress(file);
    long diff = System.currentTimeMillis() - start;
    if (diff < master.getSystemConfiguration().getTimeInMillis(Property.MASTER_RECOVERY_DELAY))
      return Math.max(diff, 0);
    FileSystem fs = master.getFileSystem();
    if (fs.exists(getSource(master)))
      return 0;
    log.warn("Unable to locate file " + file + " wal for server " + server);
    return 1000;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    Path source = getSource(master);
    FileSystem fs = master.getFileSystem();
    if (fs instanceof TraceFileSystem)
      fs = ((TraceFileSystem) fs).getImplementation();
    try {
      if (fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        dfs.recoverLease(source);
        log.info("Recovered lease on " + source.toString());
        return new SubmitFileForRecovery(server, file);
      }
    } catch (IOException ex) {
      log.error("error recovering lease ", ex);
    }
    try {
      fs.append(source).close();
      log.info("Recovered lease on " + source.toString() + " using append");

    } catch (IOException ex) {
      log.error("error recovering lease using append", ex);
    }
    // lets do this again
    return new RecoverLease(server, file);
  }
  
}
