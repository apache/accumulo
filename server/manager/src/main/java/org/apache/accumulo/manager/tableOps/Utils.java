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
package org.apache.accumulo.manager.tableOps;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.TabletMergeability;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.TabletMergeabilityUtil;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.DistributedLock;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.core.fate.zookeeper.FateLock;
import org.apache.accumulo.core.fate.zookeeper.LockRange;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooReservation;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.MoreCollectors;

public class Utils {
  private static final byte[] ZERO_BYTE = {'0'};
  private static final Logger log = LoggerFactory.getLogger(Utils.class);

  public static <T extends AbstractId<T>> T getNextId(String name, ServerContext context,
      Function<String,T> newIdFunction) throws AcceptableThriftTableOperationException {
    try {
      ZooReaderWriter zoo = context.getZooSession().asReaderWriter();
      byte[] nid = zoo.mutateOrCreate(Constants.ZTABLES, ZERO_BYTE, currentValue -> {
        BigInteger nextId = new BigInteger(new String(currentValue, UTF_8), Character.MAX_RADIX);
        nextId = nextId.add(BigInteger.ONE);
        return nextId.toString(Character.MAX_RADIX).getBytes(UTF_8);
      });
      return newIdFunction.apply(new String(nid, UTF_8));
    } catch (Exception e1) {
      log.error("Failed to assign id to " + name, e1);
      throw new AcceptableThriftTableOperationException(null, name, TableOperation.CREATE,
          TableOperationExceptionType.OTHER, e1.getMessage());
    }
  }

  private static KeyExtent findContaining(Ample ample, TableId tableId, Text row) {
    Objects.requireNonNull(row);
    try (var tablets = ample.readTablets().forTable(tableId).overlapping(row, true, row)
        .fetch(TabletMetadata.ColumnType.PREV_ROW).build()) {
      return tablets.stream().collect(MoreCollectors.onlyElement()).getExtent();
    }
  }

  /**
   * Widen a range to the greatest table split that is before the range and least table split that
   * is after the range.
   */
  public static KeyExtent widen(Ample ample, KeyExtent extent) {
    Text prevRowOfStartRowTablet = extent.prevEndRow();
    Text endRowOfEndRowTablet = extent.endRow();

    if (extent.prevEndRow() != null) {
      // The startRow is not inclusive, so do not want to find the tablet containing startRow but
      // instead find the tablet that contains the next possible row after startRow
      Text nextPossibleRow = new Key(extent.prevEndRow()).followingKey(PartialKey.ROW).getRow();
      prevRowOfStartRowTablet =
          findContaining(ample, extent.tableId(), nextPossibleRow).prevEndRow();
    }

    if (extent.endRow() != null) {
      // find the tablet containing endRow and use its endRow
      endRowOfEndRowTablet = findContaining(ample, extent.tableId(), extent.endRow()).endRow();
    }

    return new KeyExtent(extent.tableId(), endRowOfEndRowTablet, prevRowOfStartRowTablet);
  }

  public static LockRange widen(Ample ample, TableId tableId, LockRange range, TableOperation op,
      boolean tableMustExists) throws AcceptableThriftTableOperationException {
    if (range.isInfinite()) {
      return range;
    }
    try {
      var wext = widen(ample, new KeyExtent(tableId, range.getEndRow(), range.getStartRow()));
      return LockRange.of(wext.prevEndRow(), wext.endRow());
    } catch (NoSuchElementException nse) {
      if (tableMustExists) {
        throw new AcceptableThriftTableOperationException(tableId.canonical(), "", op,
            TableOperationExceptionType.NOTFOUND, "Table does not exist");
      } else {
        log.debug("Attempted to widen range {} but no metadata entries found for table {}", range,
            tableId);
        return LockRange.infinite();
      }
    }

  }

  public static long reserveTable(Manager env, TableId tableId, FateId fateId, LockType lockType,
      boolean tableMustExist, TableOperation op) throws Exception {
    return reserveTable(env, tableId, fateId, lockType, tableMustExist, op, LockRange.infinite());
  }

  public static long reserveTable(Manager env, TableId tableId, FateId fateId, LockType lockType,
      boolean tableMustExist, TableOperation op, final LockRange range) throws Exception {
    final LockRange widenedRange;

    boolean shouldWiden = lockType == LockType.WRITE || op == TableOperation.COMPACT;

    if (shouldWiden) {
      /*
       * Write locks are widened to table split points to avoid non-overlapping ranges operating on
       * the same tablet. For example assume a table has splits at c,e and two fate operations need
       * write locks on ranges (a1,c2] and (d1,d9]. The fate ranges do not overlap, however both
       * will operate on the tablet (c,e]. When the ranges are widened, (a1,c2] turns into (null,e]
       * and (d1,d9] turns into (c,e]. The widened ranges for the fate operations overlap which
       * prevents both from operating on the same tablet.
       *
       * Widening is done for write locks because those need to work on mutually exclusive sets of
       * tablets w/ other write or read locks. Read locks do not need to be mutually exclusive w/
       * each other and it does not matter if they operate on the same tablet so widening is not
       * needed for that case. Widening write locks is sufficient for detecting overlap w/ any
       * tablets that read locks need to use. For example if a write locks is obtained on (a1,c2]
       * and it widens to (null,e] then a read lock on (d1,d9] would overlap with the widened range.
       *
       * Mostly widening for write locks is nice because fate operations that get read locks are
       * probably more frequent. So the work of widening is not done on most fate operations. Also
       * widening an infinite range is a quick operation, so create/delete table will not be slowed
       * down by widening.
       *
       * Widening is done for compactions because those operations widen their range.
       */
      widenedRange = widen(env.getContext().getAmple(), tableId, range, op, tableMustExist);
      log.debug("{} widened write lock range from {} to {}", fateId, range, widenedRange);
    } else {
      widenedRange = range;
    }

    var lock = getLock(env.getContext(), tableId, fateId, lockType, widenedRange);
    if (shouldWiden && !widenedRange.equals(lock.getRange())) {
      // It is possible the range changed since the lock entry was created. Pre existing locks are
      // found using the fate id and could have a different range.
      lock.unlock();
      // This should be a rare event, log at info in case it happens a lot.
      log.info(
          "{} widened range {} differs from existing lock range {}, deleted lock and will retry",
          fateId, widenedRange, lock.getRange());
      return 100;
    } else if (lockType == LockType.READ) {
      // Not expecting the lock range on a read lock to change.
      Preconditions.checkState(widenedRange.equals(lock.getRange()));
    }

    if (lock.tryLock()) {
      if (shouldWiden) {
        // Now that table lock is acquired see if the range still widens to the same thing. If not
        // it means the table splits changed so release the lock and try again later. The table
        // splits in this range can not change once the lock is acquired, so this recheck is done
        // after getting the lock.
        var widenedRange2 = widen(env.getContext().getAmple(), tableId, range, op, tableMustExist);
        if (!widenedRange.equals(widenedRange2)) {
          lock.unlock();
          log.info(
              "{} widened range {} changed to {} after acquiring lock, deleted lock and will retry",
              fateId, widenedRange, widenedRange2);
          return 100;
        }
      }

      if (tableMustExist) {
        ZooReaderWriter zk = env.getContext().getZooSession().asReaderWriter();
        if (!zk.exists(Constants.ZTABLES + "/" + tableId)) {
          throw new AcceptableThriftTableOperationException(tableId.canonical(), "", op,
              TableOperationExceptionType.NOTFOUND, "Table does not exist");
        }
      }

      if (widenedRange.equals(range)) {
        log.info("table {} {} locked for {} operation: {} range:{}", tableId, fateId, lockType, op,
            range);
      } else {
        log.info("table {} {} locked for {} operation: {} range:{} widenedRange:{}", tableId,
            fateId, lockType, op, range, widenedRange);
      }
      return 0;
    } else {
      return 100;
    }
  }

  public static void unreserveTable(Manager env, TableId tableId, FateId fateId,
      LockType lockType) {
    getLock(env.getContext(), tableId, fateId, lockType, LockRange.infinite()).unlock();
    log.info("table {} {} unlocked for {}", tableId, fateId, lockType);
  }

  public static void unreserveNamespace(Manager env, NamespaceId namespaceId, FateId fateId,
      LockType lockType) {
    getLock(env.getContext(), namespaceId, fateId, lockType, LockRange.infinite()).unlock();
    log.info("namespace {} {} unlocked for {}", namespaceId, fateId, lockType);
  }

  public static long reserveNamespace(Manager env, NamespaceId namespaceId, FateId fateId,
      LockType lockType, boolean mustExist, TableOperation op) throws Exception {
    if (getLock(env.getContext(), namespaceId, fateId, lockType, LockRange.infinite()).tryLock()) {
      if (mustExist) {
        ZooReaderWriter zk = env.getContext().getZooSession().asReaderWriter();
        if (!zk.exists(Constants.ZNAMESPACES + "/" + namespaceId)) {
          throw new AcceptableThriftTableOperationException(namespaceId.canonical(), "", op,
              TableOperationExceptionType.NAMESPACE_NOTFOUND, "Namespace does not exist");
        }
      }
      log.info("namespace {} {} locked for {} operation: {}", namespaceId, fateId, lockType, op);
      return 0;
    } else {
      return 100;
    }
  }

  public static long reserveHdfsDirectory(Manager env, String directory, FateId fateId)
      throws KeeperException, InterruptedException {

    ZooReaderWriter zk = env.getContext().getZooSession().asReaderWriter();

    if (ZooReservation.attempt(zk, Constants.ZHDFS_RESERVATIONS + "/"
        + Base64.getEncoder().encodeToString(directory.getBytes(UTF_8)), fateId, "")) {
      return 0;
    } else {
      return 50;
    }
  }

  public static void unreserveHdfsDirectory(Manager env, String directory, FateId fateId)
      throws KeeperException, InterruptedException {
    ZooReservation.release(env.getContext().getZooSession().asReaderWriter(),
        Constants.ZHDFS_RESERVATIONS + "/"
            + Base64.getEncoder().encodeToString(directory.getBytes(UTF_8)),
        fateId);
  }

  private static DistributedLock getLock(ServerContext context, AbstractId<?> id, FateId fateId,
      LockType lockType, LockRange range) {
    FateLock qlock = new FateLock(context.getZooSession().asReaderWriter(),
        FateLock.path(Constants.ZTABLE_LOCKS + "/" + id.canonical()));
    DistributedLock lock = DistributedReadWriteLock.recoverLock(qlock, fateId);
    if (lock != null) {

      // Validate the recovered lock type
      if (lock.getType() != lockType) {
        throw new IllegalStateException(
            "Unexpected lock type " + lock.getType() + " recovered for transaction " + fateId
                + " on object " + id + ". Expected " + lockType + " lock instead.");
      }
    } else {
      DistributedReadWriteLock locker = new DistributedReadWriteLock(qlock, fateId, range);
      switch (lockType) {
        case WRITE:
          lock = locker.writeLock();
          break;
        case READ:
          lock = locker.readLock();
          break;
        default:
          throw new IllegalStateException("Unexpected LockType: " + lockType);
      }
    }
    return lock;
  }

  public static DistributedLock getReadLock(Manager env, AbstractId<?> id, FateId fateId,
      LockRange range) {
    return Utils.getLock(env.getContext(), id, fateId, LockType.READ, range);
  }

  /**
   * Given a fully-qualified Path and a flag indicating if the file info is base64 encoded or not,
   * retrieve the data from a file on the file system. It is assumed that the file is textual and
   * not binary data.
   *
   * @param path the fully-qualified path
   */
  public static SortedSet<Text> getSortedSetFromFile(Manager manager, Path path, boolean encoded)
      throws IOException {
    FileSystem fs = path.getFileSystem(manager.getContext().getHadoopConf());
    var data = new TreeSet<Text>();
    try (var file = new java.util.Scanner(fs.open(path), UTF_8)) {
      while (file.hasNextLine()) {
        String line = file.nextLine();
        data.add(encoded ? new Text(Base64.getDecoder().decode(line)) : new Text(line));
      }
    }
    return data;
  }

  public static SortedMap<Text,TabletMergeability> getSortedSplitsFromFile(Manager manager,
      Path path) throws IOException {
    FileSystem fs = path.getFileSystem(manager.getContext().getHadoopConf());
    var data = new TreeMap<Text,TabletMergeability>();
    try (var file = new java.util.Scanner(fs.open(path), UTF_8)) {
      while (file.hasNextLine()) {
        String line = file.nextLine();
        log.trace("split line: {}", line);
        Pair<Text,TabletMergeability> splitTm = TabletMergeabilityUtil.decode(line);
        data.put(splitTm.getFirst(), splitTm.getSecond());
      }
    }
    return data;
  }

}
