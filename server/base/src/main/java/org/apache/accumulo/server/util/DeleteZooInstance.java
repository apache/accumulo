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
package org.apache.accumulo.server.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteZooInstance {

  private static final Logger log = LoggerFactory.getLogger(DeleteZooInstance.class);

  public static void execute(final ServerContext context, final boolean clean,
      final String instance, final String auth) throws InterruptedException, KeeperException {

    final ZooReaderWriter zk = context.getZooReaderWriter();
    if (auth != null) {
      ZooUtil.digestAuth(zk.getZooKeeper(), auth);
    }

    if (clean) {
      // If clean is set to true then a specific instance should not be set
      if (instance != null) {
        throw new IllegalArgumentException(
            "Cannot set clean flag to true and also an instance name");
      }
      cleanAllOld(context, zk);
    } else {
      // If all old is false then we require a specific instance
      Objects.requireNonNull(instance, "Instance name must not be null");
      removeInstance(context, zk, instance);
    }
  }

  private static void removeInstance(ServerContext context, final ZooReaderWriter zk,
      final String instance) throws InterruptedException, KeeperException {
    // try instance name:
    Set<String> instances = new HashSet<>(getInstances(zk));
    Set<String> uuids = new HashSet<>(zk.getChildren(Constants.ZROOT));
    uuids.remove("instances");
    if (instances.contains(instance)) {
      String path = getInstancePath(instance);
      byte[] data = zk.getData(path);
      if (data != null) {
        final String instanceId = new String(data, UTF_8);
        if (checkCurrentInstance(context, instance, instanceId)) {
          deleteRetry(zk, path);
          deleteRetry(zk, getRootChildPath(instanceId));
          System.out.println("Deleted instance: " + instance);
        }
      }
    } else if (uuids.contains(instance)) {
      // look for the real instance name
      for (String zkInstance : instances) {
        String path = getInstancePath(zkInstance);
        byte[] data = zk.getData(path);
        if (data != null) {
          final String instanceId = new String(data, UTF_8);
          if (instance.equals(instanceId) && checkCurrentInstance(context, instance, instanceId)) {
            deleteRetry(zk, path);
            System.out.println("Deleted instance: " + instance);
          }
        }
      }
      deleteRetry(zk, getRootChildPath(instance));
    }
  }

  private static void cleanAllOld(ServerContext context, final ZooReaderWriter zk)
      throws InterruptedException, KeeperException {
    for (String child : zk.getChildren(Constants.ZROOT)) {
      if (Constants.ZINSTANCES.equals("/" + child)) {
        for (String instanceName : getInstances(zk)) {
          String instanceNamePath = getInstancePath(instanceName);
          byte[] id = zk.getData(instanceNamePath);
          if (id != null && !new String(id, UTF_8).equals(context.getInstanceID().canonical())) {
            deleteRetry(zk, instanceNamePath);
            System.out.println("Deleted instance: " + instanceName);
          }
        }
      } else if (!child.equals(context.getInstanceID().canonical())) {
        deleteRetry(zk, getRootChildPath(child));
      }
    }
  }

  private static boolean checkCurrentInstance(ServerContext context, String instanceName,
      String instanceId) {
    boolean operate = true;
    // If the instance given is the current instance we should verify the user actually wants to
    // delete
    if (instanceId.equals(context.getInstanceID().canonical())) {
      String line = String.valueOf(System.console()
          .readLine("Warning: This is the current instance, are you sure? (yes|no): "));
      operate = line != null && (line.equalsIgnoreCase("y") || line.equalsIgnoreCase("yes"));
      if (!operate) {
        System.out.println("Instance deletion of '" + instanceName + "' cancelled.");
      }
    }
    return operate;
  }

  private static String getRootChildPath(String child) {
    return Constants.ZROOT + "/" + child;
  }

  private static String getInstancePath(final String instanceName) {
    return Constants.ZROOT + Constants.ZINSTANCES + "/" + instanceName;
  }

  private static List<String> getInstances(final ZooReaderWriter zk)
      throws InterruptedException, KeeperException {
    return zk.getChildren(Constants.ZROOT + Constants.ZINSTANCES);
  }

  private static void deleteRetry(ZooReaderWriter zk, String path)
      throws InterruptedException, KeeperException {
    for (int i = 0; i < 10; i++) {
      try {
        zk.recursiveDelete(path, NodeMissingPolicy.SKIP);
        return;
      } catch (KeeperException.NotEmptyException ex) {
        if (log.isDebugEnabled()) {
          log.debug(ex.getMessage(), ex);
        }
      }
    }
  }

}
