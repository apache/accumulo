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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerDirs;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.beust.jcommander.JCommander;
import com.google.auto.service.AutoService;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

@AutoService(KeywordExecutable.class)
public class ChangeSecret extends ServerKeywordExecutable<ServerUtilOpts> {

  public ChangeSecret() {
    super(new ServerUtilOpts());
  }

  @Override
  public String keyword() {
    return "change-secret";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.ADMIN;
  }

  @Override
  public String description() {
    return "Changes the unique secret given to the instance that all servers must know.";
  }

  @Override
  public void execute(JCommander cl, ServerUtilOpts options) throws Exception {
    ServerContext context = options.getServerContext();

    try (var fs = context.getVolumeManager()) {
      ServerDirs serverDirs = new ServerDirs(context.getConfiguration(), new Configuration());
      verifyHdfsWritePermission(serverDirs, fs);

      String oldPass = String.valueOf(System.console().readPassword("Old secret: "));
      String newPass = String.valueOf(System.console().readPassword("New secret: "));

      Span span = TraceUtil.startSpan(ChangeSecret.class, "main");
      try (Scope scope = span.makeCurrent()) {

        verifyAccumuloIsDown(context, oldPass);

        final InstanceId newInstanceId = InstanceId.of(UUID.randomUUID());
        updateHdfs(serverDirs, fs, newInstanceId);
        rewriteZooKeeperInstance(context, newInstanceId, oldPass, newPass);
        if (!StringUtils.isBlank(oldPass)) {
          deleteInstance(context, oldPass);
        }
        System.out.println("New instance id is " + newInstanceId);
        System.out.println("Be sure to put your new secret in accumulo.properties");
      } finally {
        span.end();
      }
    }
  }

  private interface Visitor {
    void visit(ZooReader zoo, String path) throws Exception;
  }

  private void recurse(ZooReader zoo, String root, Visitor v) {
    try {
      v.visit(zoo, root);
      for (String child : zoo.getChildren(root)) {
        recurse(zoo, root + "/" + child, v);
      }
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

  private void verifyAccumuloIsDown(ServerContext context, String oldPassword) throws Exception {
    var conf = context.getSiteConfiguration();
    try (var oldZk =
        new ZooSession(ChangeSecret.class.getSimpleName() + ".verifyAccumuloIsDown(oldPassword)",
            conf.get(Property.INSTANCE_ZK_HOST),
            (int) conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT), oldPassword)) {
      final List<String> ephemerals = new ArrayList<>();
      recurse(oldZk.asReaderWriter(), "", (zoo, path) -> {
        Stat stat = zoo.getStatus(path);
        if (stat.getEphemeralOwner() != 0) {
          ephemerals.add(path);
        }
      });
      if (!ephemerals.isEmpty()) {
        System.err.println("The following ephemeral nodes exist, something is still running:");
        for (String path : ephemerals) {
          System.err.println(path);
        }
        throw new Exception("Accumulo must be shut down in order to run this tool.");
      }
    }
  }

  private void rewriteZooKeeperInstance(final ServerContext context, final InstanceId newInstanceId,
      String oldPass, String newPass) throws Exception {
    var conf = context.getSiteConfiguration();
    try (
        var oldZk = new ZooSession(
            ChangeSecret.class.getSimpleName() + ".rewriteZooKeeperInstance(oldPass)",
            conf.get(Property.INSTANCE_ZK_HOST),
            (int) conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT), oldPass);
        var newZk = new ZooSession(
            ChangeSecret.class.getSimpleName() + ".rewriteZooKeeperInstance(newPass)",
            conf.get(Property.INSTANCE_ZK_HOST),
            (int) conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT), newPass)) {

      final var orig = oldZk.asReaderWriter();
      final var new_ = newZk.asReaderWriter();
      recurse(orig, "", (zoo, path) -> {
        String newPath =
            path.replace(context.getInstanceID().canonical(), newInstanceId.canonical());
        byte[] data = zoo.getData(path);
        List<ACL> acls = oldZk.getACL(path, new Stat());
        if (acls.containsAll(Ids.READ_ACL_UNSAFE)) {
          new_.putPersistentData(newPath, data, NodeExistsPolicy.FAIL);
        } else {
          // upgrade
          if (acls.containsAll(Ids.OPEN_ACL_UNSAFE)) {
            // make user nodes private, they contain the user's password
            String[] parts = path.split("/");
            if (parts[parts.length - 2].equals("users")) {
              new_.putPrivatePersistentData(newPath, data, NodeExistsPolicy.FAIL);
            } else {
              // everything else can have the readable acl
              new_.putPersistentData(newPath, data, NodeExistsPolicy.FAIL);
            }
          } else {
            new_.putPrivatePersistentData(newPath, data, NodeExistsPolicy.FAIL);
          }
        }
      });
      String path = Constants.ZROOT + Constants.ZINSTANCES + "/" + context.getInstanceName();
      orig.recursiveDelete(path, NodeMissingPolicy.SKIP);
      new_.putPersistentData(path, newInstanceId.canonical().getBytes(UTF_8),
          NodeExistsPolicy.OVERWRITE);

    }
  }

  private void updateHdfs(ServerDirs serverDirs, VolumeManager fs, InstanceId newInstanceId)
      throws IOException {
    // Need to recreate the instanceId on all of them to keep consistency
    for (Volume v : fs.getVolumes()) {
      final Path instanceId = serverDirs.getInstanceIdLocation(v);
      if (!v.getFileSystem().delete(instanceId, true)) {
        throw new IOException("Could not recursively delete " + instanceId);
      }

      if (!v.getFileSystem().mkdirs(instanceId)) {
        throw new IOException("Could not create directory " + instanceId);
      }

      v.getFileSystem().create(new Path(instanceId, newInstanceId.canonical())).close();
    }
  }

  private void verifyHdfsWritePermission(ServerDirs serverDirs, VolumeManager fs) throws Exception {
    for (Volume v : fs.getVolumes()) {
      final Path instanceId = serverDirs.getInstanceIdLocation(v);
      FileStatus fileStatus = v.getFileSystem().getFileStatus(instanceId);
      checkHdfsAccessPermissions(fileStatus, FsAction.WRITE);
    }
  }

  private void checkHdfsAccessPermissions(FileStatus stat, FsAction mode) throws Exception {
    FsPermission perm = stat.getPermission();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String user = ugi.getShortUserName();
    List<String> groups = Arrays.asList(ugi.getGroupNames());
    if (user.equals(stat.getOwner())) {
      if (perm.getUserAction().implies(mode)) {
        return;
      }
    } else if (groups.contains(stat.getGroup())) {
      if (perm.getGroupAction().implies(mode)) {
        return;
      }
    } else {
      if (perm.getOtherAction().implies(mode)) {
        return;
      }
    }
    throw new Exception(String.format("Permission denied: user=%s, path=\"%s\":%s:%s:%s%s", user,
        stat.getPath(), stat.getOwner(), stat.getGroup(), stat.isDirectory() ? "d" : "-", perm));
  }

  private void deleteInstance(ServerContext context, String oldPass) throws Exception {
    var conf = context.getSiteConfiguration();
    try (var oldZk = new ZooSession(ChangeSecret.class.getSimpleName() + ".deleteInstance()",
        conf.get(Property.INSTANCE_ZK_HOST),
        (int) conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT), oldPass)) {

      var orig = oldZk.asReaderWriter();
      orig.recursiveDelete("", NodeMissingPolicy.SKIP);
    }
  }
}
