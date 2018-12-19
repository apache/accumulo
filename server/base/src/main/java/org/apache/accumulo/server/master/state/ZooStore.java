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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.List;

import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooStore implements DistributedStore {

  private static final Logger log = LoggerFactory.getLogger(ZooStore.class);

  private ServerContext context;
  private String basePath;
  private ZooCache cache;

  public ZooStore(ServerContext context) {
    this.context = context;
    cache = new ZooCache(context.getZooReaderWriter(), null);
    String zkRoot = context.getZooKeeperRoot();
    if (zkRoot.endsWith("/"))
      zkRoot = zkRoot.substring(0, zkRoot.length() - 1);
    this.basePath = zkRoot;
  }

  @Override
  public byte[] get(String path) throws DistributedStoreException {
    try {
      return cache.get(relative(path));
    } catch (Exception ex) {
      throw new DistributedStoreException(ex);
    }
  }

  private String relative(String path) {
    return basePath + path;
  }

  @Override
  public List<String> getChildren(String path) throws DistributedStoreException {
    try {
      return cache.getChildren(relative(path));
    } catch (Exception ex) {
      throw new DistributedStoreException(ex);
    }
  }

  @Override
  public void put(String path, byte[] bs) throws DistributedStoreException {
    try {
      path = relative(path);
      context.getZooReaderWriter().putPersistentData(path, bs, NodeExistsPolicy.OVERWRITE);
      cache.clear();
      log.debug("Wrote {} to {}", new String(bs, UTF_8), path);
    } catch (Exception ex) {
      throw new DistributedStoreException(ex);
    }
  }

  @Override
  public void remove(String path) throws DistributedStoreException {
    try {
      log.debug("Removing {}", path);
      path = relative(path);
      IZooReaderWriter zoo = context.getZooReaderWriter();
      if (zoo.exists(path))
        zoo.recursiveDelete(path, NodeMissingPolicy.SKIP);
      cache.clear();
    } catch (Exception ex) {
      throw new DistributedStoreException(ex);
    }
  }
}
