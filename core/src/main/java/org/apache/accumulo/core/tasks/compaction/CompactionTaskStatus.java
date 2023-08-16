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

import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.tasks.TaskMessage;
import org.apache.accumulo.core.tasks.TaskMessageType;
import org.apache.accumulo.core.tasks.ThriftSerializers;
import org.apache.accumulo.core.util.json.GsonIgnore;
import org.apache.thrift.TException;

public class CompactionTaskStatus extends TaskMessage {

  private byte[] status;

  @GsonIgnore
  private TExternalCompaction thriftObj;

  public CompactionTaskStatus() {}

  @Override
  public TaskMessageType getMessageType() {
    return TaskMessageType.COMPACTION_TASK_STATUS;
  }

  public void setCompactionStatus(TExternalCompaction status) throws TException {
    this.status = ThriftSerializers.EXTERNAL_COMPACTION_STATUS_SERIALIZER.get().serialize(status);
    this.thriftObj = status;
  }

  public TExternalCompaction getCompactionStatus() throws TException {
    if (this.thriftObj == null && this.status != null) {
      TExternalCompaction obj = new TExternalCompaction();
      ThriftSerializers.EXTERNAL_COMPACTION_STATUS_SERIALIZER.get().deserialize(obj, status);
      this.thriftObj = obj;
    }
    return this.thriftObj;
  }

}
