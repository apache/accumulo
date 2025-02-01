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

import java.util.Arrays;
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

    FateLockEntry(LockType lockType, FateId fateId) {
      this.lockType = Objects.requireNonNull(lockType);
      this.fateId = Objects.requireNonNull(fateId);
    }

    private FateLockEntry(String entry) {
      // TODO can byte constructor be removed?
      this(entry.getBytes(UTF_8));
    }

    private FateLockEntry(byte[] entry) {
      if (entry == null || entry.length < 1) {
        throw new IllegalArgumentException();
      }

      int split = -1;
      for (int i = 0; i < entry.length; i++) {
        if (entry[i] == ':') {
          split = i;
          break;
        }
      }

      if (split == -1) {
        throw new IllegalArgumentException();
      }

      this.lockType = LockType.valueOf(new String(entry, 0, split, UTF_8));
      this.fateId =
          FateId.from(new String(Arrays.copyOfRange(entry, split + 1, entry.length), UTF_8));
    }

    public LockType getLockType() {
      return lockType;
    }

    public FateId getFateId() {
      return fateId;
    }

    public byte[] serialize() {
      // TODO redo for serialization into name
      byte[] typeBytes = lockType.name().getBytes(UTF_8);
      byte[] fateIdBytes = fateId.canonical().getBytes(UTF_8);
      byte[] result = new byte[fateIdBytes.length + 1 + typeBytes.length];
      System.arraycopy(typeBytes, 0, result, 0, typeBytes.length);
      result[typeBytes.length] = ':';
      System.arraycopy(fateIdBytes, 0, result, typeBytes.length + 1, fateIdBytes.length);
      return result;
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

    public static FateLockEntry deserialize(byte[] serialized) {
      return new FateLockEntry(serialized);
    }

    @Override
    public int compareTo(FateLockEntry o) {
      // TODO determine how it sorted before this change
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

  // TODO rename to NodeName
  public static class FateLockNode implements Comparable<FateLockNode> {
    public final long sequence;
    public final FateLockEntry lockData;

    FateLockNode(String nodeName) {
      int len = nodeName.length();
      Preconditions.checkArgument(nodeName.startsWith(PREFIX) && nodeName.charAt(len - 11) == '#',
          "Illegal node name %s", nodeName);
      sequence = Long.parseUnsignedLong(nodeName.substring(len - 10), 10);
      lockData = new FateLockEntry(nodeName.substring(PREFIX.length(), len - 11));
    }

    @Override
    public int compareTo(FateLockNode o) {
      int cmp = Long.compare(sequence, o.sequence);
      if (cmp == 0) {
        cmp = lockData.compareTo(o.lockData);
      }
      return cmp;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof FateLockNode) {
        return this.compareTo((FateLockNode) o) == 0;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(sequence, lockData);
    }
  }

  @Override
  public long addEntry(FateLockEntry entry) {

    String dataString = new String(entry.serialize(), UTF_8);
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
        var parsed = new FateLockNode(name);
        // TODO supplier probably not need becaue always parsing now
        if (predicate.test(parsed.sequence, () -> parsed.lockData)) {
          // Use a supplier so we don't need to deserialize unless the calling code cares about
          // the value for that entry.
          result.put(parsed.sequence, Suppliers.memoize(() -> parsed.lockData));
        }
      }
    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException(ex);
    }
    return result;
  }

  @Override
  public void removeEntry(FateLockEntry data, long entry) {
    String dataString = new String(data.serialize(), UTF_8);
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
  public static SortedSet<FateLockNode> validateAndWarn(FateLockPath path, List<String> children) {
    log.trace("validating and sorting children at path {}", path);

    SortedSet<FateLockNode> validChildren = new TreeSet<>();

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
