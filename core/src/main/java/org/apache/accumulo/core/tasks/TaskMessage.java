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

import org.apache.accumulo.core.tasks.thrift.Task;
import org.apache.accumulo.core.util.json.ByteArrayToBase64TypeAdapter;
import org.apache.accumulo.core.util.json.GsonIgnoreExclusionStrategy;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Generic object that can be used to transport TaskMessage subclasses over Thrift using the Thrift
 * Task type. Implementations of this class are serialized to JSON then transported via Task and
 * deserialized on the other side.
 */
public abstract class TaskMessage {

  public static TaskMessage fromThriftTask(Task to) {
    TaskMessageType type = TaskMessageType.valueOf(to.getMessageType());
    return TaskMessage.GSON_FOR_TASKS.fromJson(to.getMessage(), type.getTaskClass());
  }

  public static final Gson GSON_FOR_TASKS =
      new GsonBuilder().setExclusionStrategies(new GsonIgnoreExclusionStrategy())
          .registerTypeHierarchyAdapter(byte[].class, new ByteArrayToBase64TypeAdapter()).create();

  private String taskId;
  private long fateTxId;

  public TaskMessage() {}

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

  public abstract TaskMessageType getMessageType();

  public Task toThriftTask() {
    Task t = new Task();
    t.setMessageType(getMessageType().name());
    t.setMessage(TaskMessage.GSON_FOR_TASKS.toJson(this));
    return t;
  }

}
