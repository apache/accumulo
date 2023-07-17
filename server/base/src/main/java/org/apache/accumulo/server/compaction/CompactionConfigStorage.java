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
package org.apache.accumulo.server.compaction;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.UserCompactionUtils;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;

public class CompactionConfigStorage {

  private static String createPath(ServerContext context, long fateTxId) {
    String txidString = FastFormat.toHexString(fateTxId);
    return context.getZooKeeperRoot() + Constants.ZCOMPACTIONS + "/" + txidString;
  }

  public static byte[] encodeConfig(CompactionConfig config, TableId tableId) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {
      dos.writeUTF(tableId.canonical());
      UserCompactionUtils.encode(dos, config);
      dos.close();
      return baos.toByteArray();
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  public static CompactionConfig getConfig(ServerContext context, long fateTxId)
      throws InterruptedException, KeeperException {
    return getConfig(context, fateTxId, tableId -> true);
  }

  public static CompactionConfig getConfig(ServerContext context, long fateTxId,
      Predicate<TableId> tableIdPredicate) throws InterruptedException, KeeperException {
    try {
      byte[] data = context.getZooReaderWriter().getData(createPath(context, fateTxId));
      try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
          DataInputStream dis = new DataInputStream(bais)) {
        var tableId = TableId.of(dis.readUTF());
        if (tableIdPredicate.test(tableId)) {
          return UserCompactionUtils.decodeCompactionConfig(dis);
        } else {
          return null;
        }
      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      }

    } catch (KeeperException.NoNodeException e) {
      return null;
    }
  }

  public static void setConfig(ServerContext context, long fateTxId, byte[] encConfig)
      throws InterruptedException, KeeperException {
    context.getZooReaderWriter().putPrivatePersistentData(createPath(context, fateTxId), encConfig,
        ZooUtil.NodeExistsPolicy.SKIP);
  }

  public static void deleteConfig(ServerContext context, long fateTxId)
      throws InterruptedException, KeeperException {
    context.getZooReaderWriter().delete(createPath(context, fateTxId));
  }

  public static Map<Long,CompactionConfig> getAllConfig(ServerContext context,
      Predicate<TableId> tableIdPredicate) throws InterruptedException, KeeperException {

    Map<Long,CompactionConfig> configs = new HashMap<>();

    var children = context.getZooReaderWriter()
        .getChildren(context.getZooKeeperRoot() + Constants.ZCOMPACTIONS);
    for (var child : children) {
      var fateTxid = Long.parseLong(child, 16);
      var cconf = getConfig(context, fateTxid, tableIdPredicate);
      if (cconf != null) {
        configs.put(fateTxid, cconf);
      }
    }

    return Collections.unmodifiableMap(configs);
  }

}
