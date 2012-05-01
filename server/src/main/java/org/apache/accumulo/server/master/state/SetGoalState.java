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
package org.apache.accumulo.server.master.state;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.FileSystem;

public class SetGoalState {
  
  /**
   * Utility program that will change the goal state for the master from the command line.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1 || MasterGoalState.valueOf(args[0]) == null) {
      System.err.println("Usage: accumulo " + SetGoalState.class.getName() + " [NORMAL|SAFE_MODE|CLEAN_STOP]");
      System.exit(-1);
    }
    SecurityUtil.serverLogin();

    FileSystem fs = FileUtil.getFileSystem(CachedConfiguration.getInstance(), ServerConfiguration.getSiteConfiguration());
    Accumulo.waitForZookeeperAndHdfs(fs);
    ZooReaderWriter.getInstance().putPersistentData(ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZMASTER_GOAL_STATE, args[0].getBytes(),
        NodeExistsPolicy.OVERWRITE);
  }
  
}
