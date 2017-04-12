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
package org.apache.accumulo.server.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.beust.jcommander.Parameter;

public class ChangeSecret {

  static class Opts extends ClientOpts {
    @Parameter(names = "--old", description = "old zookeeper password", password = true, hidden = true)
    String oldPass;
    @Parameter(names = "--new", description = "new zookeeper password", password = true, hidden = true)
    String newPass;
  }

  public static void main(String[] args) throws Exception {
    VolumeManager fs = VolumeManagerImpl.get();
    verifyHdfsWritePermission(fs);

    Opts opts = new Opts();
    List<String> argsList = new ArrayList<>(args.length + 2);
    argsList.add("--old");
    argsList.add("--new");
    argsList.addAll(Arrays.asList(args));
    opts.parseArgs(ChangeSecret.class.getName(), argsList.toArray(new String[0]));

    Instance inst = opts.getInstance();
    verifyAccumuloIsDown(inst, opts.oldPass);

    final String newInstanceId = UUID.randomUUID().toString();
    updateHdfs(fs, inst, newInstanceId);
    rewriteZooKeeperInstance(inst, newInstanceId, opts.oldPass, opts.newPass);
    if (opts.oldPass != null) {
      deleteInstance(inst, opts.oldPass);
    }
    System.out.println("New instance id is " + newInstanceId);
    System.out.println("Be sure to put your new secret in accumulo-site.xml");
  }

  interface Visitor {
    void visit(ZooReader zoo, String path) throws Exception;
  }

  private static void recurse(ZooReader zoo, String root, Visitor v) {
    try {
      v.visit(zoo, root);
      for (String child : zoo.getChildren(root)) {
        recurse(zoo, root + "/" + child, v);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private static void verifyAccumuloIsDown(Instance inst, String oldPassword) throws Exception {
    ZooReader zooReader = new ZooReaderWriter(inst.getZooKeepers(), inst.getZooKeepersSessionTimeOut(), oldPassword);
    String root = ZooUtil.getRoot(inst);
    final List<String> ephemerals = new ArrayList<>();
    recurse(zooReader, root, new Visitor() {
      @Override
      public void visit(ZooReader zoo, String path) throws Exception {
        Stat stat = zoo.getStatus(path);
        if (stat.getEphemeralOwner() != 0)
          ephemerals.add(path);
      }
    });
    if (ephemerals.size() > 0) {
      System.err.println("The following ephemeral nodes exist, something is still running:");
      for (String path : ephemerals) {
        System.err.println(path);
      }
      throw new Exception("Accumulo must be shut down in order to run this tool.");
    }
  }

  private static void rewriteZooKeeperInstance(final Instance inst, final String newInstanceId, String oldPass, String newPass) throws Exception {
    final ZooReaderWriter orig = new ZooReaderWriter(inst.getZooKeepers(), inst.getZooKeepersSessionTimeOut(), oldPass);
    final IZooReaderWriter new_ = new ZooReaderWriter(inst.getZooKeepers(), inst.getZooKeepersSessionTimeOut(), newPass);

    String root = ZooUtil.getRoot(inst);
    recurse(orig, root, new Visitor() {
      @Override
      public void visit(ZooReader zoo, String path) throws Exception {
        String newPath = path.replace(inst.getInstanceID(), newInstanceId);
        byte[] data = zoo.getData(path, null);
        List<ACL> acls = orig.getZooKeeper().getACL(path, new Stat());
        if (acls.containsAll(Ids.READ_ACL_UNSAFE)) {
          new_.putPersistentData(newPath, data, NodeExistsPolicy.FAIL);
        } else {
          // upgrade
          if (acls.containsAll(Ids.OPEN_ACL_UNSAFE)) {
            // make user nodes private, they contain the user's password
            String parts[] = path.split("/");
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
      }
    });
    String path = "/accumulo/instances/" + inst.getInstanceName();
    orig.recursiveDelete(path, NodeMissingPolicy.SKIP);
    new_.putPersistentData(path, newInstanceId.getBytes(UTF_8), NodeExistsPolicy.OVERWRITE);
  }

  private static void updateHdfs(VolumeManager fs, Instance inst, String newInstanceId) throws IOException {
    // Need to recreate the instanceId on all of them to keep consistency
    for (Volume v : fs.getVolumes()) {
      final Path instanceId = ServerConstants.getInstanceIdLocation(v);
      if (!v.getFileSystem().delete(instanceId, true)) {
        throw new IOException("Could not recursively delete " + instanceId);
      }

      if (!v.getFileSystem().mkdirs(instanceId)) {
        throw new IOException("Could not create directory " + instanceId);
      }

      v.getFileSystem().create(new Path(instanceId, newInstanceId)).close();
    }
  }

  private static void verifyHdfsWritePermission(VolumeManager fs) throws Exception {
    for (Volume v : fs.getVolumes()) {
      final Path instanceId = ServerConstants.getInstanceIdLocation(v);
      FileStatus fileStatus = v.getFileSystem().getFileStatus(instanceId);
      checkHdfsAccessPermissions(fileStatus, FsAction.WRITE);
    }
  }

  private static void checkHdfsAccessPermissions(FileStatus stat, FsAction mode) throws Exception {
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
    throw new Exception(String.format("Permission denied: user=%s, path=\"%s\":%s:%s:%s%s", user, stat.getPath(), stat.getOwner(), stat.getGroup(),
        stat.isDirectory() ? "d" : "-", perm));
  }

  private static void deleteInstance(Instance origInstance, String oldPass) throws Exception {
    IZooReaderWriter orig = new ZooReaderWriter(origInstance.getZooKeepers(), origInstance.getZooKeepersSessionTimeOut(), oldPass);
    orig.recursiveDelete("/accumulo/" + origInstance.getInstanceID(), NodeMissingPolicy.SKIP);
  }
}
