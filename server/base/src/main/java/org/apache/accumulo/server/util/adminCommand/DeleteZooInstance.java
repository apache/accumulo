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
package org.apache.accumulo.server.util.adminCommand;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.server.util.adminCommand.DeleteZooInstance.DeleteZooInstanceOpts;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class DeleteZooInstance extends ServerKeywordExecutable<DeleteZooInstanceOpts> {

  private static final Logger log = LoggerFactory.getLogger(DeleteZooInstance.class);

  static class DeleteZooInstanceOpts extends ServerUtilOpts {
    @Parameter(names = {"-i", "--instance"}, description = "the instance name or id to delete")
    String instance;

    @Parameter(names = {"-c", "--clean"},
        description = "Cleans Zookeeper by deleting all old instances. This will not delete the instance pointed to by the local accumulo.properties file")
    boolean clean = false;

    @Parameter(names = {"--password"},
        description = "The system secret, if different than instance.secret in accumulo.properties",
        password = true)
    String auth;
  }

  public DeleteZooInstance() {
    super(new DeleteZooInstanceOpts());
  }

  @Override
  public String keyword() {
    return "delete-instance";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroup.ADMIN;
  }

  @Override
  public String description() {
    return "Deletes specific instance name or id from zookeeper or cleans up all old instances.";
  }

  @Override
  public void execute(JCommander cl, DeleteZooInstanceOpts options) throws Exception {

    ServerContext context = options.getServerContext();

    if (options.auth != null) {
      context.getZooSession().addAccumuloDigestAuth(options.auth);
    }

    if (options.clean) {
      // If clean is set to true then a specific instance should not be set
      if (options.instance != null) {
        throw new IllegalArgumentException(
            "Cannot set clean flag to true and also an instance name");
      }
      cleanAllOld(context);
    } else {
      // If all old is false then we require a specific instance
      Objects.requireNonNull(options.instance, "Instance name must not be null");
      removeInstance(context, options.instance);
    }
  }

  private void removeInstance(ServerContext context, final String instance)
      throws InterruptedException, KeeperException {
    var zrw = context.getZooSession().asReaderWriter();
    // try instance name:
    Set<String> instances = new HashSet<>(getInstances(zrw));
    Set<String> uuids = new HashSet<>(zrw.getChildren(Constants.ZROOT));
    uuids.remove("instances");
    if (instances.contains(instance)) {
      String path = getInstancePath(instance);
      byte[] data = zrw.getData(path);
      if (data != null) {
        final String instanceId = new String(data, UTF_8);
        if (checkCurrentInstance(context, instance, instanceId)) {
          deleteRetry(zrw, path);
          deleteRetry(zrw, getRootChildPath(instanceId));
          System.out.println("Deleted instance: " + instance);
        }
      }
    } else if (uuids.contains(instance)) {
      // look for the real instance name
      for (String zkInstance : instances) {
        String path = getInstancePath(zkInstance);
        byte[] data = zrw.getData(path);
        if (data != null) {
          final String instanceId = new String(data, UTF_8);
          if (instance.equals(instanceId) && checkCurrentInstance(context, instance, instanceId)) {
            deleteRetry(zrw, path);
            System.out.println("Deleted instance: " + instance);
          }
        }
      }
      deleteRetry(zrw, getRootChildPath(instance));
    }
  }

  private void cleanAllOld(ServerContext context) throws InterruptedException, KeeperException {
    var zrw = context.getZooSession().asReaderWriter();
    for (String child : zrw.getChildren(Constants.ZROOT)) {
      if (Constants.ZINSTANCES.equals("/" + child)) {
        for (String instanceName : getInstances(zrw)) {
          String instanceNamePath = getInstancePath(instanceName);
          byte[] id = zrw.getData(instanceNamePath);
          if (id != null && !new String(id, UTF_8).equals(context.getInstanceID().canonical())) {
            deleteRetry(zrw, instanceNamePath);
            System.out.println("Deleted instance: " + instanceName);
          }
        }
      } else if (!child.equals(context.getInstanceID().canonical())) {
        deleteRetry(zrw, getRootChildPath(child));
      }
    }
  }

  private boolean checkCurrentInstance(ServerContext context, String instanceName,
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

  private String getRootChildPath(String child) {
    return Constants.ZROOT + "/" + child;
  }

  private String getInstancePath(final String instanceName) {
    return Constants.ZROOT + Constants.ZINSTANCES + "/" + instanceName;
  }

  private List<String> getInstances(final ZooReaderWriter zk)
      throws InterruptedException, KeeperException {
    return zk.getChildren(Constants.ZROOT + Constants.ZINSTANCES);
  }

  private void deleteRetry(ZooReaderWriter zk, String path)
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
