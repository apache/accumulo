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
package org.apache.accumulo.manager.tableOps.clone;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.tableOps.AbstractFateOperation;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.slf4j.LoggerFactory;

class CloneMetadata extends AbstractFateOperation {

  private static final long serialVersionUID = 1L;
  private final CloneInfo cloneInfo;

  public CloneMetadata(CloneInfo cloneInfo) {
    this.cloneInfo = cloneInfo;
  }

  @Override
  public long isReady(FateId fateId, FateEnv environment) {
    return 0;
  }

  @Override
  public Repo<FateEnv> call(FateId fateId, FateEnv environment) throws Exception {
    LoggerFactory.getLogger(CloneMetadata.class)
        .info(String.format("Cloning %s with tableId %s from srcTableId %s",
            cloneInfo.getTableName(), cloneInfo.getTableId(), cloneInfo.getSrcTableId()));
    // need to clear out any metadata entries for tableId just in case this
    // died before and is executing again
    MetadataTableUtil.deleteTable(cloneInfo.getTableId(), false, environment.getContext(),
        environment.getServiceLock());
    MetadataTableUtil.cloneTable(environment.getContext(), cloneInfo.getSrcTableId(),
        cloneInfo.getTableId());
    return new FinishCloneTable(cloneInfo);
  }

  @Override
  public void undo(FateId fateId, FateEnv environment) throws Exception {
    MetadataTableUtil.deleteTable(cloneInfo.getTableId(), false, environment.getContext(),
        environment.getServiceLock());
  }

}
