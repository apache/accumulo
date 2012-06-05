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
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.fate.Repo;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.master.tableOps.MasterRepo;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * 
 */
public class SubmitFileForRecovery extends MasterRepo implements Repo<Master> {
  
  private static final long serialVersionUID = 1L;
  String server;
  String file;
  
  SubmitFileForRecovery(String server, String file) {
    this.server = server;
    this.file = file;
  }

  @Override
  public Repo<Master> call(long tid, final Master master) throws Exception {
    master.updateRecoveryInProgress(file);
    String source = RecoverLease.getSource(master, server, file).toString();
    ZooReaderWriter zoo = ZooReaderWriter.getInstance();
    final String path = ZooUtil.getRoot(master.getInstance()) + Constants.ZRECOVERY + "/" + file;
    zoo.putPersistentData(path, source.getBytes(), NodeExistsPolicy.SKIP);
    log.info("Created zookeeper entry " + path + " with data " + source);
    zoo.exists(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        switch (event.getType()) {
          case NodeDataChanged:
            log.info("noticed recovery entry for " + file + " was removed");
            FileSystem fs = master.getFileSystem();
            Path finished = new Path(Constants.getRecoveryDir(master.getSystemConfiguration()), "finished");
            try {
              if (fs.exists(finished))
                log.info("log recovery for " + file + " successful");
              else
                log.error("zookeeper recovery entry " + path + " has been removed, but the finish flag file is missing");
            } catch (IOException ex) {
              log.error("Unable to check on the recovery status of " + file, ex);
            }
            break;
        }
      }
      
    });
    return null;
  }
  
}
