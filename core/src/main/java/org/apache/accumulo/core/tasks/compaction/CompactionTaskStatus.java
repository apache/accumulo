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

import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.tasks.TaskMessage;
import org.apache.accumulo.core.tasks.ThriftSerializers;
import org.apache.accumulo.core.util.json.GsonIgnore;
import org.apache.thrift.TException;

public class CompactionTaskStatus extends TaskMessage {

  private byte[] status;

  @GsonIgnore
  private TCompactionStatusUpdate statusThriftObj;

  public CompactionTaskStatus() {}

  public void setCompactionStatus(TCompactionStatusUpdate status) throws TException {
    this.status = ThriftSerializers.EXTERNAL_COMPACTION_STATUS_SERIALIZER.get().serialize(status);
    this.statusThriftObj = status;
  }

  public TCompactionStatusUpdate getCompactionStatus() throws TException {
    if (this.statusThriftObj == null && this.status != null) {
      TCompactionStatusUpdate obj = new TCompactionStatusUpdate();
      ThriftSerializers.EXTERNAL_COMPACTION_STATUS_SERIALIZER.get().deserialize(obj, status);
      this.statusThriftObj = obj;
    }
    return this.statusThriftObj;
  }

}
