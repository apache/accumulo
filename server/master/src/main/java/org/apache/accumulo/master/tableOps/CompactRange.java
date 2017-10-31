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
import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;
import org.apache.accumulo.core.client.impl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.client.impl.CompactionStrategyConfigUtil;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter.Mutator;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.master.tableOps.UserCompactionConfig;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactRange extends MasterRepo {
  private static final Logger log = LoggerFactory.getLogger(CompactRange.class);

  private static final long serialVersionUID = 1L;
  private final Table.ID tableId;
  private final Namespace.ID namespaceId;
  private byte[] startRow;
  private byte[] endRow;
  private byte[] config;

  public CompactRange(Namespace.ID namespaceId, Table.ID tableId, byte[] startRow, byte[] endRow, List<IteratorSetting> iterators,
      CompactionStrategyConfig compactionStrategy) throws AcceptableThriftTableOperationException {

    requireNonNull(namespaceId, "Invalid argument: null namespaceId");
    requireNonNull(tableId, "Invalid argument: null tableId");
    requireNonNull(iterators, "Invalid argument: null iterator list");
    requireNonNull(compactionStrategy, "Invalid argument: null compactionStrategy");

    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.startRow = startRow.length == 0 ? null : startRow;
    this.endRow = endRow.length == 0 ? null : endRow;

    if (iterators.size() > 0 || !compactionStrategy.equals(CompactionStrategyConfigUtil.DEFAULT_STRATEGY)) {
      this.config = WritableUtils.toByteArray(new UserCompactionConfig(this.startRow, this.endRow, iterators, compactionStrategy));
    } else {
      log.info("No iterators or compaction strategy");
    }

    if (this.startRow != null && this.endRow != null && new Text(startRow).compareTo(new Text(endRow)) >= 0)
      throw new AcceptableThriftTableOperationException(tableId.canonicalID(), null, TableOperation.COMPACT, TableOperationExceptionType.BAD_RANGE,
          "start row must be less than end row");
  }

  @Override
  public long isReady(long tid, Master env) throws Exception {
    return Utils.reserveNamespace(namespaceId, tid, false, true, TableOperation.COMPACT)
        + Utils.reserveTable(tableId, tid, false, true, TableOperation.COMPACT);
  }

  @Override
  public Repo<Master> call(final long tid, Master env) throws Exception {
    String zTablePath = Constants.ZROOT + "/" + env.getInstance().getInstanceID() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_COMPACT_ID;

    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    byte[] cid;
    try {
      cid = zoo.mutate(zTablePath, null, null, new Mutator() {
        @Override
        public byte[] mutate(byte[] currentValue) throws Exception {
          String cvs = new String(currentValue, UTF_8);
          String[] tokens = cvs.split(",");
          long flushID = Long.parseLong(tokens[0]);
          flushID++;

          String txidString = String.format("%016x", tid);

          for (int i = 1; i < tokens.length; i++) {
            if (tokens[i].startsWith(txidString))
              continue; // skip self

            log.debug("txidString : {}", txidString);
            log.debug("tokens[{}] : {}", i, tokens[i]);

            throw new AcceptableThriftTableOperationException(tableId.canonicalID(), null, TableOperation.COMPACT, TableOperationExceptionType.OTHER,
                "Another compaction with iterators and/or a compaction strategy is running");
          }

          StringBuilder encodedIterators = new StringBuilder();

          if (config != null) {
            Hex hex = new Hex();
            encodedIterators.append(",");
            encodedIterators.append(txidString);
            encodedIterators.append("=");
            encodedIterators.append(new String(hex.encode(config), UTF_8));
          }

          return (Long.toString(flushID) + encodedIterators).getBytes(UTF_8);
        }
      });

      return new CompactionDriver(Long.parseLong(new String(cid, UTF_8).split(",")[0]), namespaceId, tableId, startRow, endRow);
    } catch (NoNodeException nne) {
      throw new AcceptableThriftTableOperationException(tableId.canonicalID(), null, TableOperation.COMPACT, TableOperationExceptionType.NOTFOUND, null);
    }

  }

  static void removeIterators(Master environment, final long txid, Table.ID tableId) throws Exception {
    String zTablePath = Constants.ZROOT + "/" + environment.getInstance().getInstanceID() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_COMPACT_ID;

    IZooReaderWriter zoo = ZooReaderWriter.getInstance();

    zoo.mutate(zTablePath, null, null, new Mutator() {
      @Override
      public byte[] mutate(byte[] currentValue) throws Exception {
        String cvs = new String(currentValue, UTF_8);
        String[] tokens = cvs.split(",");
        long flushID = Long.parseLong(tokens[0]);

        String txidString = String.format("%016x", txid);

        StringBuilder encodedIterators = new StringBuilder();
        for (int i = 1; i < tokens.length; i++) {
          if (tokens[i].startsWith(txidString))
            continue;
          encodedIterators.append(",");
          encodedIterators.append(tokens[i]);
        }

        return (Long.toString(flushID) + encodedIterators).getBytes(UTF_8);
      }
    });

  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    try {
      removeIterators(env, tid, tableId);
    } finally {
      Utils.unreserveNamespace(namespaceId, tid, false);
      Utils.unreserveTable(tableId, tid, false);
    }
  }

}
