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
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock;
import org.apache.accumulo.core.fate.zookeeper.FateLock;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooReservation;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  private static final byte[] ZERO_BYTE = {'0'};
  private static final Logger log = LoggerFactory.getLogger(Utils.class);

  public static void checkTableDoesNotExist(ServerContext context, String tableName,
      TableId tableId, TableOperation operation) throws AcceptableThriftTableOperationException {

    TableId id = context.getTableNameToIdMap().get(tableName);

    if (id != null && !id.equals(tableId)) {
      throw new AcceptableThriftTableOperationException(null, tableName, operation,
          TableOperationExceptionType.EXISTS, null);
    }
  }

  public static <T extends AbstractId<T>> T getNextId(String name, ServerContext context,
      Function<String,T> newIdFunction) throws AcceptableThriftTableOperationException {
    try {
      ZooReaderWriter zoo = context.getZooReaderWriter();
      final String ntp = context.getZooKeeperRoot() + Constants.ZTABLES;
      byte[] nid = zoo.mutateOrCreate(ntp, ZERO_BYTE, currentValue -> {
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

  static final Lock tableNameLock = new ReentrantLock();
  static final Lock idLock = new ReentrantLock();

  public static long reserveTable(Manager env, TableId tableId, long tid, boolean writeLock,
      boolean tableMustExist, TableOperation op) throws Exception {
    if (getLock(env.getContext(), tableId, tid, writeLock).tryLock()) {
      if (tableMustExist) {
        ZooReaderWriter zk = env.getContext().getZooReaderWriter();
        if (!zk.exists(env.getContext().getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId)) {
          throw new AcceptableThriftTableOperationException(tableId.canonical(), "", op,
              TableOperationExceptionType.NOTFOUND, "Table does not exist");
        }
      }
      log.info("table {} {} locked for {} operation: {}", tableId, FateTxId.formatTid(tid),
          (writeLock ? "write" : "read"), op);
      return 0;
    } else {
      return 100;
    }
  }

  public static void unreserveTable(Manager env, TableId tableId, long tid, boolean writeLock) {
    getLock(env.getContext(), tableId, tid, writeLock).unlock();
    log.info("table {} {} unlocked for {}", tableId, FateTxId.formatTid(tid),
        (writeLock ? "write" : "read"));
  }

  public static void unreserveNamespace(Manager env, NamespaceId namespaceId, long id,
      boolean writeLock) {
    getLock(env.getContext(), namespaceId, id, writeLock).unlock();
    log.info("namespace {} {} unlocked for {}", namespaceId, FateTxId.formatTid(id),
        (writeLock ? "write" : "read"));
  }

  public static long reserveNamespace(Manager env, NamespaceId namespaceId, long id,
      boolean writeLock, boolean mustExist, TableOperation op) throws Exception {
    if (getLock(env.getContext(), namespaceId, id, writeLock).tryLock()) {
      if (mustExist) {
        ZooReaderWriter zk = env.getContext().getZooReaderWriter();
        if (!zk.exists(
            env.getContext().getZooKeeperRoot() + Constants.ZNAMESPACES + "/" + namespaceId)) {
          throw new AcceptableThriftTableOperationException(namespaceId.canonical(), "", op,
              TableOperationExceptionType.NAMESPACE_NOTFOUND, "Namespace does not exist");
        }
      }
      log.info("namespace {} {} locked for {} operation: {}", namespaceId, FateTxId.formatTid(id),
          (writeLock ? "write" : "read"), op);
      return 0;
    } else {
      return 100;
    }
  }

  public static long reserveHdfsDirectory(Manager env, String directory, long tid)
      throws KeeperException, InterruptedException {
    String resvPath = env.getContext().getZooKeeperRoot() + Constants.ZHDFS_RESERVATIONS + "/"
        + Base64.getEncoder().encodeToString(directory.getBytes(UTF_8));

    ZooReaderWriter zk = env.getContext().getZooReaderWriter();

    if (ZooReservation.attempt(zk, resvPath, FastFormat.toHexString(tid), "")) {
      return 0;
    } else {
      return 50;
    }
  }

  public static void unreserveHdfsDirectory(Manager env, String directory, long tid)
      throws KeeperException, InterruptedException {
    String resvPath = env.getContext().getZooKeeperRoot() + Constants.ZHDFS_RESERVATIONS + "/"
        + Base64.getEncoder().encodeToString(directory.getBytes(UTF_8));
    ZooReservation.release(env.getContext().getZooReaderWriter(), resvPath,
        FastFormat.toHexString(tid));
  }

  private static Lock getLock(ServerContext context, AbstractId<?> id, long tid,
      boolean writeLock) {
    byte[] lockData = FastFormat.toZeroPaddedHex(tid);
    var fLockPath =
        FateLock.path(context.getZooKeeperRoot() + Constants.ZTABLE_LOCKS + "/" + id.canonical());
    FateLock qlock = new FateLock(context.getZooReaderWriter(), fLockPath);
    Lock lock = DistributedReadWriteLock.recoverLock(qlock, lockData);
    if (lock == null) {
      DistributedReadWriteLock locker = new DistributedReadWriteLock(qlock, lockData);
      if (writeLock) {
        lock = locker.writeLock();
      } else {
        lock = locker.readLock();
      }
    }
    return lock;
  }

  public static Lock getIdLock() {
    return idLock;
  }

  public static Lock getTableNameLock() {
    return tableNameLock;
  }

  public static Lock getReadLock(Manager env, AbstractId<?> id, long tid) {
    return Utils.getLock(env.getContext(), id, tid, false);
  }

  public static void checkNamespaceDoesNotExist(ServerContext context, String namespace,
      NamespaceId namespaceId, TableOperation operation)
      throws AcceptableThriftTableOperationException {

    NamespaceId n = Namespaces.lookupNamespaceId(context, namespace);

    if (n != null && !n.equals(namespaceId)) {
      throw new AcceptableThriftTableOperationException(null, namespace, operation,
          TableOperationExceptionType.NAMESPACE_EXISTS, null);
    }
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

}
