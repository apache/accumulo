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
package org.apache.accumulo.core.fate.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiPredicate;

import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.QueueLock;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A persistent lock mechanism in ZooKeeper used for locking tables during FaTE operations.
 */
public class FateLock implements QueueLock {
  private static final Logger log = LoggerFactory.getLogger(FateLock.class);

  private static final String PREFIX = "flock#";

  private final ZooReaderWriter zoo;
  private final FateLockPath path;

  public static class FateLockPath {
    private final String path;

    private FateLockPath(String path) {
      this.path = requireNonNull(path);
    }

    @Override
    public String toString() {
      return this.path;
    }
  }

  public static FateLockPath path(String path) {
    return new FateLockPath(path);
  }

  public FateLock(ZooReaderWriter zrw, FateLockPath path) {
    this.zoo = requireNonNull(zrw);
    this.path = requireNonNull(path);
  }

  public static class FateLockNode {
    public final long sequence;
    public final String lockData;

    private FateLockNode(String nodeName) {
      int len = nodeName.length();
      Preconditions.checkArgument(nodeName.startsWith(PREFIX) && nodeName.charAt(len - 11) == '#',
          "Illegal node name %s", nodeName);
      sequence = Long.parseLong(nodeName.substring(len - 10));
      lockData = nodeName.substring(PREFIX.length(), len - 11);
    }
  }

  // TODO change data arg from byte[] to String.. in the rest of the code its always a String.
  @Override
  public long addEntry(byte[] data) {

    String dataString = new String(data, UTF_8);
    Preconditions.checkState(!dataString.contains("#"));

    String newPath;
    try {
      while (true) {
        try {
          newPath =
              zoo.putPersistentSequential(path + "/" + PREFIX + dataString + "#", new byte[0]);
          String[] parts = newPath.split("/");
          String last = parts[parts.length - 1];
          return new FateLockNode(last).sequence;
        } catch (NoNodeException nne) {
          // the parent does not exist so try to create it
          zoo.putPersistentData(path.toString(), new byte[] {}, NodeExistsPolicy.SKIP);
        }
      }
    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException(ex);
    }
  }

  @Override
  public SortedMap<Long,byte[]> getEntries(BiPredicate<Long,byte[]> predicate) {
    SortedMap<Long,byte[]> result = new TreeMap<>();
    try {
      List<String> children = Collections.emptyList();
      try {
        children = zoo.getChildren(path.toString());
      } catch (KeeperException.NoNodeException ex) {
        // the path does not exist (it was deleted or not created yet), that is ok there are no
        // earlier entries then
      }

      for (String name : children) {
        var parsed = new FateLockNode(name);
        byte[] data = parsed.lockData.getBytes(UTF_8);
        if (predicate.test(parsed.sequence, data)) {
          result.put(parsed.sequence, data);
        }
      }
    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException(ex);
    }
    return result;
  }

  @Override
  public void removeEntry(byte[] data, long entry) {
    String dataString = new String(data, UTF_8);
    Preconditions.checkState(!dataString.contains("#"));
    try {
      zoo.recursiveDelete(path + String.format("/%s%s#%010d", PREFIX, dataString, entry),
          NodeMissingPolicy.SKIP);
      try {
        // try to delete the parent if it has no children
        zoo.delete(path.toString());
      } catch (NotEmptyException nee) {
        // the path had other lock nodes, no big deal
      }
    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /**
   * Validate and sort child nodes at this lock path by the lock prefix
   */
  public static List<FateLockNode> validateAndWarn(FateLockPath path, List<String> children) {
    log.trace("validating and sorting children at path {}", path);

    List<FateLockNode> validChildren = new ArrayList<>();

    if (children == null || children.isEmpty()) {
      return validChildren;
    }

    children.forEach(c -> {
      log.trace("Validating {}", c);
      try {
        var fateLockNode = new FateLockNode(c);
        validChildren.add(fateLockNode);
      } catch (RuntimeException e) {
        log.warn("Illegal fate lock node {}", c, e);
      }
    });

    return validChildren;
  }
}
