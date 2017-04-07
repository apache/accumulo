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

import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.slf4j.LoggerFactory;

class CloneMetadata extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private CloneInfo cloneInfo;

  public CloneMetadata(CloneInfo cloneInfo) {
    this.cloneInfo = cloneInfo;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    LoggerFactory.getLogger(CloneMetadata.class).info(
        String.format("Cloning %s with tableId %s from srcTableId %s", cloneInfo.tableName, cloneInfo.tableId, cloneInfo.srcTableId));
    // need to clear out any metadata entries for tableId just in case this
    // died before and is executing again
    MetadataTableUtil.deleteTable(cloneInfo.tableId, false, environment, environment.getMasterLock());
    MetadataTableUtil.cloneTable(environment, cloneInfo.srcTableId, cloneInfo.tableId, environment.getFileSystem());
    return new FinishCloneTable(cloneInfo);
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    MetadataTableUtil.deleteTable(cloneInfo.tableId, false, environment, environment.getMasterLock());
  }

}
