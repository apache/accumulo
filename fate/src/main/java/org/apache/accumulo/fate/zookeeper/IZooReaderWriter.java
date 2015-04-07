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

import java.util.List;

import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

public interface IZooReaderWriter extends IZooReader {

  ZooKeeper getZooKeeper();

  void recursiveDelete(String zPath, NodeMissingPolicy policy) throws KeeperException, InterruptedException;

  /**
   * Create a persistent node with the default ACL
   *
   * @return true if the node was created or altered; false if it was skipped
   */
  boolean putPersistentData(String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException;

  boolean putPrivatePersistentData(String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException;

  void putPersistentData(String zPath, byte[] data, int version, NodeExistsPolicy policy) throws KeeperException, InterruptedException;

  boolean putPersistentData(String zPath, byte[] data, int version, NodeExistsPolicy policy, List<ACL> acls) throws KeeperException, InterruptedException;

  String putPersistentSequential(String zPath, byte[] data) throws KeeperException, InterruptedException;

  String putEphemeralSequential(String zPath, byte[] data) throws KeeperException, InterruptedException;

  String putEphemeralData(String zPath, byte[] data) throws KeeperException, InterruptedException;

  void recursiveCopyPersistent(String source, String destination, NodeExistsPolicy policy) throws KeeperException, InterruptedException;

  void delete(String path, int version) throws InterruptedException, KeeperException;

  interface Mutator {
    byte[] mutate(byte[] currentValue) throws Exception;
  }

  byte[] mutate(String zPath, byte[] createValue, List<ACL> acl, Mutator mutator) throws Exception;

  boolean isLockHeld(ZooUtil.LockID lockID) throws KeeperException, InterruptedException;

  void mkdirs(String path) throws KeeperException, InterruptedException;

  @Override
  void sync(String path) throws KeeperException, InterruptedException;

}
