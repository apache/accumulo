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
package org.apache.accumulo.master.state;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerUtil;
import org.apache.accumulo.server.security.SecurityUtil;

public class SetGoalState {

  /**
   * Utility program that will change the goal state for the master from the command line.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1 || MasterGoalState.valueOf(args[0]) == null) {
      System.err.println(
          "Usage: accumulo " + SetGoalState.class.getName() + " [NORMAL|SAFE_MODE|CLEAN_STOP]");
      System.exit(-1);
    }

    try {
      var context = new ServerContext(SiteConfiguration.auto());
      SecurityUtil.serverLogin(context.getConfiguration());
      ServerUtil.waitForZookeeperAndHdfs(context);
      context.getZooReaderWriter().putPersistentData(
          context.getZooKeeperRoot() + Constants.ZMASTER_GOAL_STATE, args[0].getBytes(UTF_8),
          NodeExistsPolicy.OVERWRITE);
    } finally {
      SingletonManager.setMode(Mode.CLOSED);
    }
  }

}
