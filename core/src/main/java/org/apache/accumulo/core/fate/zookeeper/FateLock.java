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

import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
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
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;
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

  public static class FateLockEntry {
    final LockType lockType;
    final FateId fateId;
    final LockRange range;

    private FateLockEntry(LockType lockType, FateId fateId, LockRange range) {
      this.lockType = Objects.requireNonNull(lockType);
      this.fateId = Objects.requireNonNull(fateId);
      this.range = range;
    }

    private FateLockEntry(String entry) {
      var fields = entry.split("_", 4);
      this.lockType = LockType.valueOf(fields[0]);
      this.fateId = FateId.from(fields[1]);
      this.range = LockRange.of(decode(fields[2]), decode(fields[3]));
    }

    public LockType getLockType() {
      return lockType;
    }

    public FateId getFateId() {
      return fateId;
    }

    public LockRange getRange() {
      return range;
    }

    private String encode(Text row) {
      if (row == null) {
        return "N";
      } else {
        return "P" + Base64.getEncoder().encodeToString(TextUtil.getBytes(row));
      }
    }

    private Text decode(String enc) {
      if (enc.charAt(0) == 'P') {
        return new Text(Base64.getDecoder().decode(enc.substring(1)));
      } else if (enc.charAt(0) == 'N') {
        return null;
      } else {
        throw new IllegalArgumentException("Unexpected prefix " + enc);
      }
    }

    public String serialize() {
      return lockType.name() + "_" + fateId.canonical() + "_" + encode(range.getStartRow()) + "_"
          + encode(range.getEndRow());
    }

    public static FateLockEntry from(LockType lockType, FateId fateId, LockRange range) {
      return new FateLockEntry(lockType, fateId, range);
    }

    public static FateLockEntry deserialize(String serialized) {
      return new FateLockEntry(serialized);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FateLockEntry)) {
        return false;
      }
      FateLockEntry that = (FateLockEntry) o;
      return lockType == that.lockType && Objects.equals(fateId, that.fateId)
          && Objects.equals(range, that.range);
    }

    @Override
    public int hashCode() {
      return Objects.hash(lockType, fateId, range);
    }
  }

  public static FateLockPath path(String path) {
    return new FateLockPath(path);
  }

  public FateLock(ZooReaderWriter zrw, FateLockPath path) {
    this.zoo = requireNonNull(zrw);
    this.path = requireNonNull(path);
  }

  public static class NodeName {
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
    public boolean equals(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException();
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

    SortedSet<NodeName> validChildren = new TreeSet<>(Comparator.comparingLong(nn -> nn.sequence));

    if (children == null || children.isEmpty()) {
      return validChildren;
    }

    children.forEach(c -> {
      log.trace("Validating {}", c);
      try {
        var fateLockNode = new NodeName(c);
        if (!validChildren.add(fateLockNode)) {
          log.warn("Duplicate sequence {}", c);
        }
      } catch (RuntimeException e) {
        log.warn("Illegal fate lock node {}", c, e);
      }
    });

    return validChildren;
  }
}
