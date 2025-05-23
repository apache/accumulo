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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.QueueLock;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;

/**
 * A persistent lock mechanism in ZooKeeper used for locking tables during FaTE operations.
 */
public class FateLock implements QueueLock {
  private static final Logger log = LoggerFactory.getLogger(FateLock.class);

  static final String PREFIX = "flock#";

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

  public static class FateLockEntry implements Comparable<FateLockEntry> {
    final LockType lockType;
    final FateId fateId;

    private FateLockEntry(LockType lockType, FateId fateId) {
      this.lockType = Objects.requireNonNull(lockType);
      this.fateId = Objects.requireNonNull(fateId);
    }

    private FateLockEntry(String entry) {
      var fields = entry.split(":", 2);
      this.lockType = LockType.valueOf(fields[0]);
      this.fateId = FateId.from(fields[1]);
    }

    public LockType getLockType() {
      return lockType;
    }

    public FateId getFateId() {
      return fateId;
    }

    public String serialize() {
      return lockType.name() + ":" + fateId.canonical();
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FateLockEntry lockEntry = (FateLockEntry) o;
      return lockType == lockEntry.lockType && fateId.equals(lockEntry.fateId);
    }

    @Override
    public int hashCode() {
      int result = lockType.hashCode();
      result = 31 * result + fateId.hashCode();
      return result;
    }

    public static FateLockEntry from(LockType lockType, FateId fateId) {
      return new FateLockEntry(lockType, fateId);
    }

    public static FateLockEntry deserialize(String serialized) {
      return new FateLockEntry(serialized);
    }

    @Override
    public int compareTo(FateLockEntry o) {
      int cmp = lockType.compareTo(o.lockType);
      if (cmp == 0) {
        cmp = fateId.compareTo(o.fateId);
      }
      return cmp;
    }
  }

  public static FateLockPath path(String path) {
    return new FateLockPath(path);
  }

  public FateLock(ZooReaderWriter zrw, FateLockPath path) {
    this.zoo = requireNonNull(zrw);
    this.path = requireNonNull(path);
  }

  public static class NodeName implements Comparable<NodeName> {
    public final long sequence;
    public final Supplier<FateLockEntry> fateLockEntry;

    NodeName(String nodeName) {
      int len = nodeName.length();
      Preconditions.checkArgument(nodeName.startsWith(PREFIX) && nodeName.charAt(len - 11) == '#',
          "Illegal node name %s", nodeName);
      sequence = Long.parseUnsignedLong(nodeName.substring(len - 10), 10);
      // Use a supplier so we don't need to deserialize unless the calling code cares about
      // the value for that entry.
      fateLockEntry = Suppliers
          .memoize(() -> FateLockEntry.deserialize(nodeName.substring(PREFIX.length(), len - 11)));
    }

    @Override
    public int compareTo(NodeName o) {
      int cmp = Long.compare(sequence, o.sequence);
      if (cmp == 0) {
        cmp = fateLockEntry.get().compareTo(o.fateLockEntry.get());
      }
      return cmp;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof NodeName) {
        return this.compareTo((NodeName) o) == 0;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(sequence, fateLockEntry.get());
    }
  }

  @Override
  public long addEntry(FateLockEntry entry) {

    String dataString = entry.serialize();
    Preconditions.checkState(!dataString.contains("#"));

    String newPath;
    try {
      while (true) {
        try {
          newPath =
              zoo.putPersistentSequential(path + "/" + PREFIX + dataString + "#", new byte[0]);
          String[] parts = newPath.split("/");
          String last = parts[parts.length - 1];
          return new NodeName(last).sequence;
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
  public SortedMap<Long,Supplier<FateLockEntry>>
      getEntries(BiPredicate<Long,Supplier<FateLockEntry>> predicate) {
    SortedMap<Long,Supplier<FateLockEntry>> result = new TreeMap<>();
    try {
      List<String> children = Collections.emptyList();
      try {
        children = zoo.getChildren(path.toString());
      } catch (KeeperException.NoNodeException ex) {
        // the path does not exist (it was deleted or not created yet), that is ok there are no
        // earlier entries then
      }

      for (String name : children) {
        var parsed = new NodeName(name);
        if (predicate.test(parsed.sequence, parsed.fateLockEntry)) {
          Preconditions.checkState(result.put(parsed.sequence, parsed.fateLockEntry) == null);
        }
      }
    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException(ex);
    }
    return result;
  }

  @Override
  public void removeEntry(FateLockEntry data, long entry) {
    String dataString = data.serialize();
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
  public static SortedSet<NodeName> validateAndWarn(FateLockPath path, List<String> children) {
    log.trace("validating and sorting children at path {}", path);

    SortedSet<NodeName> validChildren = new TreeSet<>();

    if (children == null || children.isEmpty()) {
      return validChildren;
    }

    children.forEach(c -> {
      log.trace("Validating {}", c);
      try {
        var fateLockNode = new NodeName(c);
        validChildren.add(fateLockNode);
      } catch (RuntimeException e) {
        log.warn("Illegal fate lock node {}", c, e);
      }
    });

    return validChildren;
  }
}
