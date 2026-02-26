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
package org.apache.accumulo.manager.tableOps.bulkVer2;

import java.util.Optional;

import org.apache.accumulo.core.clientImpl.bulk.BulkSerialize;
import org.apache.accumulo.core.clientImpl.bulk.LoadMappingIterator;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.util.bulkCommand.ListBulk.BulkState;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComputeBulkRange extends AbstractBulkFateOperation {
  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(ComputeBulkRange.class);

  public ComputeBulkRange(TableId tableId, String sourceDir, boolean setTime) {
    super(BulkInfo.create(tableId, sourceDir, setTime));
  }

  @Override
  public Repo<FateEnv> call(FateId fateId, FateEnv env) throws Exception {

    VolumeManager fs = env.getVolumeManager();
    final Path bulkDir = new Path(bulkInfo.sourceDir);

    try (LoadMappingIterator lmi =
        BulkSerialize.readLoadMapping(bulkDir.toString(), bulkInfo.tableId, fs::open)) {
      KeyExtent first = lmi.next().getKey();
      KeyExtent last = first;
      while (lmi.hasNext()) {
        last = lmi.next().getKey();
      }

      bulkInfo.firstSplit =
          Optional.ofNullable(first.prevEndRow()).map(Text::getBytes).orElse(null);
      bulkInfo.lastSplit = Optional.ofNullable(last.endRow()).map(Text::getBytes).orElse(null);

      log.trace("{} first split:{} last split:{}", fateId, first.prevEndRow(), last.endRow());

      return new PrepBulkImport(bulkInfo);
    }
  }

  @Override
  public BulkState getState() {
    return BulkState.PREPARING;
  }
}
