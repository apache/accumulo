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
package org.apache.accumulo.fate.zookeeper;

import java.security.SecurityPermission;
import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.ZooKeeperConnectionInfo;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class ZooReaderWriter extends ZooReader implements IZooReaderWriter {
  private static final Logger log = Logger.getLogger(ZooReaderWriter.class);

  private static SecurityPermission ZOOWRITER_PERMISSION = new SecurityPermission("zookeeperWriterPermission");

  private static ZooReaderWriter instance = null;
  private final String scheme;
  private final byte[] auth;
  private final ZooKeeperConnectionInfo info;

  @Override
  public ZooKeeper getZooKeeper() {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(ZOOWRITER_PERMISSION);
    }
    return getSession(keepers, timeout, scheme, auth);
  }

  public ZooReaderWriter(String string, int timeInMillis, String scheme, byte[] auth) {
    super(string, timeInMillis);
    this.scheme = scheme;
    this.auth = Arrays.copyOf(auth, auth.length);
    this.info = new ZooKeeperConnectionInfo(string, timeInMillis, scheme, this.auth);
  }

  @Override
  public void recursiveDelete(String zPath, NodeMissingPolicy policy) throws KeeperException, InterruptedException {
    ZooUtil.recursiveDelete(info, zPath, policy);
  }

  /**
   * Create a persistent node with the default ACL
   *
   * @return true if the node was created or altered; false if it was skipped
   */
  @Override
  public boolean putPersistentData(String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    return ZooUtil.putPersistentData(info, zPath, data, policy);
  }

  @Override
  public boolean putPersistentData(String zPath, byte[] data, int version, NodeExistsPolicy policy, List<ACL> acls) throws KeeperException,
      InterruptedException {
    return ZooUtil.putPersistentData(info, zPath, data, version, policy, acls);
  }

  @Override
  public boolean putPrivatePersistentData(String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    return ZooUtil.putPrivatePersistentData(info, zPath, data, policy);
  }

  @Override
  public void putPersistentData(String zPath, byte[] data, int version, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    ZooUtil.putPersistentData(info, zPath, data, version, policy);
  }

  @Override
  public String putPersistentSequential(String zPath, byte[] data) throws KeeperException, InterruptedException {
    return ZooUtil.putPersistentSequential(info, zPath, data);
  }

  @Override
  public String putEphemeralData(String zPath, byte[] data) throws KeeperException, InterruptedException {
    return ZooUtil.putEphemeralData(info, zPath, data);
  }

  @Override
  public String putEphemeralSequential(String zPath, byte[] data) throws KeeperException, InterruptedException {
    return ZooUtil.putEphemeralSequential(info, zPath, data);
  }

  @Override
  public void recursiveCopyPersistent(String source, String destination, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    ZooUtil.recursiveCopyPersistent(info, source, destination, policy);
  }

  @Override
  public void delete(String path, int version) throws InterruptedException, KeeperException {
    final Retry retry = getRetryFactory().create();
    while (true) {
      try {
        getZooKeeper().delete(path, version);
        return;
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.NONODE) {
          if (retry.hasRetried()) {
            // A retried delete could have deleted the node, assume that was the case
            log.debug("Delete saw no node on a retry. Assuming node was deleted");
            return;
          }

          throw e;
        } else if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT || code == Code.SESSIONEXPIRED) {
          // retry if we have more attempts to do so
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  @Override
  public byte[] mutate(String zPath, byte[] createValue, List<ACL> acl, Mutator mutator) throws Exception {
    if (createValue != null) {
      while (true) {
        final Retry retry = getRetryFactory().create();
        try {
          getZooKeeper().create(zPath, createValue, acl, CreateMode.PERSISTENT);
          return createValue;
        } catch (KeeperException ex) {
          final Code code = ex.code();
          if (code == Code.NODEEXISTS) {
            // expected
            break;
          } else if (code == Code.OPERATIONTIMEOUT || code == Code.CONNECTIONLOSS || code == Code.SESSIONEXPIRED) {
            retryOrThrow(retry, ex);
          } else {
            throw ex;
          }
        }

        retry.waitForNextAttempt();
      }
    }
    do {
      final Retry retry = getRetryFactory().create();
      Stat stat = new Stat();
      byte[] data = getData(zPath, false, stat);
      data = mutator.mutate(data);
      if (data == null)
        return data;
      try {
        getZooKeeper().setData(zPath, data, stat.getVersion());
        return data;
      } catch (KeeperException ex) {
        final Code code = ex.code();
        if (code == Code.BADVERSION) {
          // Retry, but don't increment. This makes it backwards compatible with the infinite
          // loop that previously happened. I'm not sure if that's really desirable though.
        } else if (code == Code.OPERATIONTIMEOUT || code == Code.CONNECTIONLOSS || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, ex);
          retry.waitForNextAttempt();
        } else {
          throw ex;
        }
      }
    } while (true);
  }

  public static synchronized ZooReaderWriter getInstance(String zookeepers, int timeInMillis, String scheme, byte[] auth) {
    if (instance == null)
      instance = new ZooReaderWriter(zookeepers, timeInMillis, scheme, auth);
    return instance;
  }

  @Override
  public boolean isLockHeld(ZooUtil.LockID lockID) throws KeeperException, InterruptedException {
    return ZooUtil.isLockHeld(info, lockID);
  }

  @Override
  public void mkdirs(String path) throws KeeperException, InterruptedException {
    if (path.equals(""))
      return;
    if (!path.startsWith("/"))
      throw new IllegalArgumentException(path + "does not start with /");
    if (exists(path))
      return;
    String parent = path.substring(0, path.lastIndexOf("/"));
    mkdirs(parent);
    putPersistentData(path, new byte[] {}, NodeExistsPolicy.SKIP);
  }

}
