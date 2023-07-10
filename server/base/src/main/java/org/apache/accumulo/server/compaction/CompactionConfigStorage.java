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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.UserCompactionUtils;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerContext;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.zookeeper.KeeperException;

public class CompactionConfigStorage {
  public static Pair<Long,CompactionConfig> getCompactionID(ServerContext context, KeyExtent extent)
      throws KeeperException.NoNodeException {
    try {
      String zTablePath = Constants.ZROOT + "/" + context.getInstanceID() + Constants.ZTABLES + "/"
          + extent.tableId() + Constants.ZTABLE_COMPACT_ID;

      String[] tokens =
          new String(context.getZooReaderWriter().getData(zTablePath), UTF_8).split(",");
      long compactID = Long.parseLong(tokens[0]);

      CompactionConfig overlappingConfig = null;

      if (tokens.length > 1) {
        Hex hex = new Hex();
        ByteArrayInputStream bais =
            new ByteArrayInputStream(hex.decode(tokens[1].split("=")[1].getBytes(UTF_8)));
        DataInputStream dis = new DataInputStream(bais);

        var compactionConfig = UserCompactionUtils.decodeCompactionConfig(dis);

        KeyExtent ke = new KeyExtent(extent.tableId(), compactionConfig.getEndRow(),
            compactionConfig.getStartRow());

        if (ke.overlaps(extent)) {
          overlappingConfig = compactionConfig;
        }
      }

      if (overlappingConfig == null) {
        overlappingConfig = new CompactionConfig(); // no config present, set to default
      }

      return new Pair<>(compactID, overlappingConfig);
    } catch (InterruptedException | DecoderException | NumberFormatException e) {
      throw new RuntimeException("Exception on " + extent + " getting compaction ID", e);
    } catch (KeeperException ke) {
      if (ke instanceof KeeperException.NoNodeException) {
        throw (KeeperException.NoNodeException) ke;
      } else {
        throw new RuntimeException("Exception on " + extent + " getting compaction ID", ke);
      }
    }
  }
}
