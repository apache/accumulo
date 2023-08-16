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

import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tasks.TaskMessage;
import org.apache.accumulo.core.tasks.TaskMessageType;
import org.apache.accumulo.core.tasks.ThriftSerializers;
import org.apache.accumulo.core.util.json.GsonIgnore;
import org.apache.thrift.TException;

public class CompactionTask extends TaskMessage {

  public byte[] job;

  @GsonIgnore
  private TExternalCompactionJob thriftJob;

  public CompactionTask() {}

  @Override
  public TaskMessageType getMessageType() {
    return TaskMessageType.COMPACTION_TASK;
  }

  public void setCompactionJob(TExternalCompactionJob job) throws TException {
    this.job = ThriftSerializers.EXTERNAL_COMPACTION_JOB_SERIALIZER.get().serialize(job);
    this.thriftJob = job;
  }

  public TExternalCompactionJob getCompactionJob() throws TException {
    if (this.thriftJob == null && this.job != null) {
      TExternalCompactionJob obj = new TExternalCompactionJob();
      ThriftSerializers.EXTERNAL_COMPACTION_JOB_SERIALIZER.get().deserialize(obj, job);
      this.thriftJob = obj;
    }
    return this.thriftJob;
  }

}
