/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.manager.tserverOps;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.manager.tableOps.AbstractFateOperation;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

public class BeginTserverShutdown extends AbstractFateOperation {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(BeginTserverShutdown.class);

  private final ResourceGroupId resourceGroup;
  private final HostAndPort hostAndPort;
  private final String serverSession;
  private final boolean force;

  public BeginTserverShutdown(TServerInstance server, ResourceGroupId resourceGroup,
      boolean force) {
    this.hostAndPort = server.getHostAndPort();
    this.resourceGroup = resourceGroup;
    this.serverSession = server.getSession();
    this.force = force;
  }

  static String createPath(HostAndPort hostPort, String session) {
    return Constants.ZSHUTTING_DOWN_TSERVERS + "/"
        + new TServerInstance(hostPort, session).getHostPortSession();
  }

  @Override
  public Repo<FateEnv> call(FateId fateId, FateEnv environment) throws Exception {
    if (!force) {
      String path = createPath(hostAndPort, serverSession);
      // Because these fate operation are seeded with a fate key there should only ever be one fate
      // operation running for a tserver instance, so do not need to worry about race conditions
      // here
      environment.getContext().getZooSession().asReaderWriter().putPersistentData(path, new byte[0],
          ZooUtil.NodeExistsPolicy.SKIP);
      log.trace("{} created {}", fateId, path);
    }
    return new ShutdownTServer(hostAndPort, serverSession, resourceGroup, force);
  }

  @Override
  public void undo(FateId fateId, FateEnv environment) throws Exception {
    if (!force) {
      String path = createPath(hostAndPort, serverSession);
      environment.getContext().getZooSession().asReaderWriter().delete(path);
      log.trace("{} removed {}", fateId, path);
    }
  }
}
