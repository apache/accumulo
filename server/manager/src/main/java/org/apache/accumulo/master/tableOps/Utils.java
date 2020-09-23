/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.tableOps;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.DistributedReadWriteLock;
import org.apache.accumulo.fate.zookeeper.ZooQueueLock;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooReservation;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  private static final byte[] ZERO_BYTE = {'0'};
  private static final Logger log = LoggerFactory.getLogger(Utils.class);

  public static void checkTableDoesNotExist(ServerContext context, String tableName,
      TableId tableId, TableOperation operation) throws AcceptableThriftTableOperationException {

    TableId id = Tables.getNameToIdMap(context).get(tableName);

    if (id != null && !id.equals(tableId))
      throw new AcceptableThriftTableOperationException(null, tableName, operation,
          TableOperationExceptionType.EXISTS, null);
  }

  public static <T extends AbstractId<T>> T getNextId(String name, ServerContext context,
      Function<String,T> newIdFunction) throws AcceptableThriftTableOperationException {
    try {
      ZooReaderWriter zoo = context.getZooReaderWriter();
      final String ntp = context.getZooKeeperRoot() + Constants.ZTABLES;
      byte[] nid = zoo.mutate(ntp, ZERO_BYTE, ZooUtil.PUBLIC, currentValue -> {
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

  public static long reserveTable(Master env, TableId tableId, long tid, boolean writeLock,
      boolean tableMustExist, TableOperation op) throws Exception {
    if (getLock(env.getContext(), tableId, tid, writeLock).tryLock()) {
      if (tableMustExist) {
        ZooReaderWriter zk = env.getContext().getZooReaderWriter();
        if (!zk.exists(env.getContext().getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId))
          throw new AcceptableThriftTableOperationException(tableId.canonical(), "", op,
              TableOperationExceptionType.NOTFOUND, "Table does not exist");
      }
      log.info("table {} ({}) locked for {} operation: {}", tableId, Long.toHexString(tid),
          (writeLock ? "write" : "read"), op);
      return 0;
    } else
      return 100;
  }

  public static void unreserveTable(Master env, TableId tableId, long tid, boolean writeLock) {
    getLock(env.getContext(), tableId, tid, writeLock).unlock();
    log.info("table {} ({}) unlocked for {}", tableId, Long.toHexString(tid),
        (writeLock ? "write" : "read"));
  }

  public static void unreserveNamespace(Master env, NamespaceId namespaceId, long id,
      boolean writeLock) {
    getLock(env.getContext(), namespaceId, id, writeLock).unlock();
    log.info("namespace {} ({}) unlocked for {}", namespaceId, Long.toHexString(id),
        (writeLock ? "write" : "read"));
  }

  public static long reserveNamespace(Master env, NamespaceId namespaceId, long id,
      boolean writeLock, boolean mustExist, TableOperation op) throws Exception {
    if (getLock(env.getContext(), namespaceId, id, writeLock).tryLock()) {
      if (mustExist) {
        ZooReaderWriter zk = env.getContext().getZooReaderWriter();
        if (!zk.exists(
            env.getContext().getZooKeeperRoot() + Constants.ZNAMESPACES + "/" + namespaceId))
          throw new AcceptableThriftTableOperationException(namespaceId.canonical(), "", op,
              TableOperationExceptionType.NAMESPACE_NOTFOUND, "Namespace does not exist");
      }
      log.info("namespace {} ({}) locked for {} operation: {}", namespaceId, Long.toHexString(id),
          (writeLock ? "write" : "read"), op);
      return 0;
    } else
      return 100;
  }

  public static long reserveHdfsDirectory(Master env, String directory, long tid)
      throws KeeperException, InterruptedException {
    String resvPath = env.getContext().getZooKeeperRoot() + Constants.ZHDFS_RESERVATIONS + "/"
        + Base64.getEncoder().encodeToString(directory.getBytes(UTF_8));

    ZooReaderWriter zk = env.getContext().getZooReaderWriter();

    if (ZooReservation.attempt(zk, resvPath, String.format("%016x", tid), "")) {
      return 0;
    } else
      return 50;
  }

  public static void unreserveHdfsDirectory(Master env, String directory, long tid)
      throws KeeperException, InterruptedException {
    String resvPath = env.getContext().getZooKeeperRoot() + Constants.ZHDFS_RESERVATIONS + "/"
        + Base64.getEncoder().encodeToString(directory.getBytes(UTF_8));
    ZooReservation.release(env.getContext().getZooReaderWriter(), resvPath,
        String.format("%016x", tid));
  }

  private static Lock getLock(ServerContext context, AbstractId<?> id, long tid,
      boolean writeLock) {
    byte[] lockData = String.format("%016x", tid).getBytes(UTF_8);
    ZooQueueLock qlock = new ZooQueueLock(context.getZooReaderWriter(),
        context.getZooKeeperRoot() + Constants.ZTABLE_LOCKS + "/" + id.canonical(), false);
    Lock lock = DistributedReadWriteLock.recoverLock(qlock, lockData);
    if (lock == null) {
      DistributedReadWriteLock locker = new DistributedReadWriteLock(qlock, lockData);
      if (writeLock)
        lock = locker.writeLock();
      else
        lock = locker.readLock();
    }
    return lock;
  }

  public static Lock getIdLock() {
    return idLock;
  }

  public static Lock getTableNameLock() {
    return tableNameLock;
  }

  public static Lock getReadLock(Master env, AbstractId<?> id, long tid) {
    return Utils.getLock(env.getContext(), id, tid, false);
  }

  public static void checkNamespaceDoesNotExist(ServerContext context, String namespace,
      NamespaceId namespaceId, TableOperation operation)
      throws AcceptableThriftTableOperationException {

    NamespaceId n = Namespaces.lookupNamespaceId(context, namespace);

    if (n != null && !n.equals(namespaceId))
      throw new AcceptableThriftTableOperationException(null, namespace, operation,
          TableOperationExceptionType.NAMESPACE_EXISTS, null);
  }

  /**
   * Given an input stream and a flag indicating if the file info is base64 encoded or not, retrieve
   * the data from a file on the file system. It is assumed that the file is textual and not binary
   * data.
   */
  public static SortedSet<Text> getSortedSetFromFile(FSDataInputStream inputStream, boolean encoded)
      throws IOException {
    SortedSet<Text> data = new TreeSet<>();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (encoded)
          data.add(new Text(Base64.getDecoder().decode(line)));
        else
          data.add(new Text(line));
      }
    }
    return data;
  }
}
