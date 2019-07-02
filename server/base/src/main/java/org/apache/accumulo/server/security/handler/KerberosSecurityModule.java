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
package org.apache.accumulo.server.security.handler;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Base64;

import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosSecurityModule extends SecurityModuleImpl {
  private static Logger log = LoggerFactory.getLogger(KerberosSecurityModule.class);

  public KerberosSecurityModule(ServerContext context) {
    super(context);
  }

  @Override
  public void initialize(String rootUser, byte[] token) {
    try {
      // remove old settings from zookeeper first, if any
      IZooReaderWriter zoo = context.getZooReaderWriter();
      synchronized (zooCache) {
        zooCache.clear();
        if (zoo.exists(ZKUserPath)) {
          zoo.recursiveDelete(ZKUserPath, ZooUtil.NodeMissingPolicy.SKIP);
          log.info("Removed {}/ from zookeeper", ZKUserPath);
        }

        // prep parent node of users with root username
        // ACCUMULO-4140 The root user needs to be stored un-base64 encoded in the znode's value
        byte[] principalData = rootUser.getBytes(UTF_8);
        zoo.putPersistentData(ZKUserPath, principalData, ZooUtil.NodeExistsPolicy.FAIL);

        // Create the root user in ZK using base64 encoded name
        createUserNodeInZk(Base64.getEncoder().encodeToString(principalData));
      }
    } catch (KeeperException | InterruptedException e) {
      log.error("Failed to initialize security", e);
      throw new RuntimeException(e);
    }
  }

  private void createUserNodeInZk(String principal) throws KeeperException, InterruptedException {
    synchronized (zooCache) {
      zooCache.clear();
      IZooReaderWriter zoo = context.getZooReaderWriter();
      zoo.putPrivatePersistentData(ZKUserPath + "/" + principal, new byte[0],
          ZooUtil.NodeExistsPolicy.FAIL);
    }
  }

  @Override
  public Auth auth() {
    return new KerberosAuthImpl(zooCache, context, ZKUserPath);
  }

  @Override
  public Perm perm() {
    return new KerberosPermImpl(zooCache, context, ZKUserPath);
  }
}
