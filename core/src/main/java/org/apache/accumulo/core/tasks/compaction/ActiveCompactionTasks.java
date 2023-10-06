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
package org.apache.accumulo.core.tasks.compaction;

import org.apache.accumulo.core.tabletserver.thrift.ActiveCompactionList;
import org.apache.accumulo.core.tasks.ThriftSerializers;
import org.apache.accumulo.core.util.json.GsonIgnore;
import org.apache.thrift.TException;

public class ActiveCompactionTasks extends CompactionTask {

  private byte[] activeCompactions;

  @GsonIgnore
  private ActiveCompactionList thriftCompactions;

  public ActiveCompactionTasks() {}

  public void setActiveCompactions(ActiveCompactionList compactions) throws TException {
    this.activeCompactions =
        ThriftSerializers.EXTERNAL_COMPACTION_ACTIVE_COMPACTION_LIST.get().serialize(compactions);
    this.thriftCompactions = compactions;
  }

  public ActiveCompactionList getActiveCompactions() throws TException {
    if (this.activeCompactions == null && this.thriftCompactions != null) {
      ActiveCompactionList obj = new ActiveCompactionList();
      ThriftSerializers.EXTERNAL_COMPACTION_ACTIVE_COMPACTION_LIST.get().deserialize(obj,
          activeCompactions);
      this.thriftCompactions = obj;
    }
    return thriftCompactions;
  }

}
