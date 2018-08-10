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
package org.apache.accumulo.master.tableOps;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.math.BigInteger;
import java.util.Base64;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.AbstractId;
import org.apache.accumulo.core.client.impl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.DistributedReadWriteLock;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooReservation;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.zookeeper.ZooQueueLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  private static final byte[] ZERO_BYTE = {'0'};
  private static final Logger log = LoggerFactory.getLogger(Utils.class);

  static void checkTableDoesNotExist(ClientContext context, String tableName, Table.ID tableId,
      TableOperation operation) throws AcceptableThriftTableOperationException {

    Table.ID id = Tables.getNameToIdMap(context).get(tableName);

    if (id != null && !id.equals(tableId))
      throw new AcceptableThriftTableOperationException(null, tableName, operation,
          TableOperationExceptionType.EXISTS, null);
  }

  static <T extends AbstractId> T getNextId(String name, ClientContext context,
      Function<String,T> newIdFunction) throws AcceptableThriftTableOperationException {
    try {
      IZooReaderWriter zoo = ZooReaderWriter.getInstance();
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

  public static long reserveTable(Master env, Table.ID tableId, long tid, boolean writeLock,
      boolean tableMustExist, TableOperation op) throws Exception {
    if (getLock(env.getContext(), tableId, tid, writeLock).tryLock()) {
      if (tableMustExist) {
        IZooReaderWriter zk = ZooReaderWriter.getInstance();
        if (!zk.exists(env.getContext().getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId))
          throw new AcceptableThriftTableOperationException(tableId.canonicalID(), "", op,
              TableOperationExceptionType.NOTFOUND, "Table does not exist");
      }
      log.info("table {} ({}) locked for {} operation: {}", tableId, Long.toHexString(tid),
          (writeLock ? "write" : "read"), op);
      return 0;
    } else
      return 100;
  }

  public static void unreserveTable(Master env, Table.ID tableId, long tid, boolean writeLock)
      throws Exception {
    getLock(env.getContext(), tableId, tid, writeLock).unlock();
    log.info("table {} ({}) unlocked for ", tableId, Long.toHexString(tid),
        (writeLock ? "write" : "read"));
  }

  public static void unreserveNamespace(Master env, Namespace.ID namespaceId, long id,
      boolean writeLock) throws Exception {
    getLock(env.getContext(), namespaceId, id, writeLock).unlock();
    log.info("namespace {} ({}) unlocked for {}", namespaceId, Long.toHexString(id),
        (writeLock ? "write" : "read"));
  }

  public static long reserveNamespace(Master env, Namespace.ID namespaceId, long id,
      boolean writeLock, boolean mustExist, TableOperation op) throws Exception {
    if (getLock(env.getContext(), namespaceId, id, writeLock).tryLock()) {
      if (mustExist) {
        IZooReaderWriter zk = ZooReaderWriter.getInstance();
        if (!zk.exists(
            env.getContext().getZooKeeperRoot() + Constants.ZNAMESPACES + "/" + namespaceId))
          throw new AcceptableThriftTableOperationException(namespaceId.canonicalID(), "", op,
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

    IZooReaderWriter zk = ZooReaderWriter.getInstance();

    if (ZooReservation.attempt(zk, resvPath, String.format("%016x", tid), "")) {
      return 0;
    } else
      return 50;
  }

  public static void unreserveHdfsDirectory(Master env, String directory, long tid)
      throws KeeperException, InterruptedException {
    String resvPath = env.getContext().getZooKeeperRoot() + Constants.ZHDFS_RESERVATIONS + "/"
        + Base64.getEncoder().encodeToString(directory.getBytes(UTF_8));
    ZooReservation.release(ZooReaderWriter.getInstance(), resvPath, String.format("%016x", tid));
  }

  private static Lock getLock(ClientContext context, AbstractId id, long tid, boolean writeLock)
      throws Exception {
    byte[] lockData = String.format("%016x", tid).getBytes(UTF_8);
    ZooQueueLock qlock = new ZooQueueLock(
        context.getZooKeeperRoot() + Constants.ZTABLE_LOCKS + "/" + id, false);
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

  public static Lock getReadLock(Master env, AbstractId tableId, long tid) throws Exception {
    return Utils.getLock(env.getContext(), tableId, tid, false);
  }

  static void checkNamespaceDoesNotExist(ClientContext context, String namespace,
      Namespace.ID namespaceId, TableOperation operation)
      throws AcceptableThriftTableOperationException {

    Namespace.ID n = Namespaces.lookupNamespaceId(context, namespace);

    if (n != null && !n.equals(namespaceId))
      throw new AcceptableThriftTableOperationException(null, namespace, operation,
          TableOperationExceptionType.NAMESPACE_EXISTS, null);
  }
}
