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
package org.apache.accumulo.core.tasks;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

@JsonTypeInfo(use = Id.CLASS, include = As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = CompactionTask.class, name = "CompactionTask"),
  @JsonSubTypes.Type(value = CompactionTaskStatus.class, name = "CompactionTaskStatus"),
})
public abstract class Task {

  private String taskId;
  private long fateTxId;
  private TaskType type;
  private byte[] serializedThriftObject;

  public Task() {}

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public long getFateTxId() {
    return fateTxId;
  }

  public void setFateTxId(long fateTxId) {
    this.fateTxId = fateTxId;
  }

  public TaskType getType() {
    return type;
  }

  public void setType(TaskType type) {
    this.type = type;
  }

  public byte[] getSerializedThriftObject() {
    return serializedThriftObject;
  }

  public void setSerializedThriftObject(byte[] serializedThriftObject) {
    this.serializedThriftObject = serializedThriftObject;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (fateTxId ^ (fateTxId >>> 32));
    result = prime * result + Arrays.hashCode(serializedThriftObject);
    result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Task other = (Task) obj;
    if (fateTxId != other.fateTxId)
      return false;
    if (!Arrays.equals(serializedThriftObject, other.serializedThriftObject))
      return false;
    if (taskId == null) {
      if (other.taskId != null)
        return false;
    } else if (!taskId.equals(other.taskId))
      return false;
    if (type != other.type)
      return false;
    return true;
  }

}
