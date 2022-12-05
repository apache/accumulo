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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.QueueLock;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  @Override
  public long addEntry(byte[] data) {
    String newPath;
    try {
      while (true) {
        try {
          newPath = zoo.putPersistentSequential(path + "/" + PREFIX, data);
          String[] parts = newPath.split("/");
          String last = parts[parts.length - 1];
          return Long.parseLong(last.substring(PREFIX.length()));
        } catch (NoNodeException nne) {
          // the parent does not exist so try to create it
          zoo.putPersistentData(path.toString(), new byte[] {}, NodeExistsPolicy.SKIP);
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public SortedMap<Long,byte[]> getEarlierEntries(long entry) {
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
        // this try catch must be done inside the loop because some subset of the children may exist
        try {
          byte[] data = zoo.getData(path + "/" + name);
          long order = Long.parseLong(name.substring(PREFIX.length()));
          if (order <= entry) {
            result.put(order, data);
          }
        } catch (KeeperException.NoNodeException ex) {
          // ignored
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return result;
  }

  @Override
  public void removeEntry(long entry) {
    try {
      zoo.recursiveDelete(path + String.format("/%s%010d", PREFIX, entry), NodeMissingPolicy.SKIP);
      try {
        // try to delete the parent if it has no children
        zoo.delete(path.toString());
      } catch (NotEmptyException nee) {
        // the path had other lock nodes, no big deal
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Validate and sort child nodes at this lock path by the lock prefix
   */
  public static List<String> validateAndSort(FateLockPath path, List<String> children) {
    log.trace("validating and sorting children at path {}", path);
    List<String> validChildren = new ArrayList<>();
    if (children == null || children.isEmpty()) {
      return validChildren;
    }
    children.forEach(c -> {
      log.trace("Validating {}", c);
      if (c.startsWith(PREFIX)) {
        int idx = c.indexOf('#');
        String sequenceNum = c.substring(idx + 1);
        if (sequenceNum.length() == 10) {
          try {
            log.trace("Testing number format of {}", sequenceNum);
            Integer.parseInt(sequenceNum);
            validChildren.add(c);
          } catch (NumberFormatException e) {
            log.warn("Fate lock found with invalid sequence number format: {} (not a number)", c);
          }
        } else {
          log.warn("Fate lock found with invalid sequence number format: {} (not 10 characters)",
              c);
        }
      } else {
        log.warn("Fate lock found with invalid lock format: {} (does not start with {})", c,
            PREFIX);
      }
    });

    if (validChildren.size() > 1) {
      validChildren.sort((o1, o2) -> {
        // Lock should be of the form:
        // lock-sequenceNumber
        // Example:
        // flock#0000000000

        // Lock length - sequenceNumber length
        // 16 - 10
        int secondHashIdx = 6;
        return Integer.valueOf(o1.substring(secondHashIdx))
            .compareTo(Integer.valueOf(o2.substring(secondHashIdx)));
      });
    }
    log.trace("Children nodes (size: {}): {}", validChildren.size(), validChildren);
    return validChildren;
  }
}
