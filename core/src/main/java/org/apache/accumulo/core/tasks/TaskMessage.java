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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Generic object that can be used to transport TaskMessage subclasses over Thrift using the Thrift
 * Task type. Implementations of this class are serialized to JSON then transported via Task and
 * deserialized on the other side.
 */
public abstract class TaskMessage {

  private static final Logger LOG = LoggerFactory.getLogger(TaskMessage.class);

  @SuppressWarnings("unchecked")
  public static <T extends TaskMessage> T fromThiftTask(Task task, TaskMessageType expectedType) {
    TaskMessageType type = TaskMessageType.valueOf(task.getMessageType());
    Preconditions.checkState(type == expectedType,
        "Task is of type: " + type + ", expected: " + expectedType);
    T decodedMsg = (T) TaskMessage.GSON_FOR_TASKS.fromJson(task.getMessage(), type.getTaskClass());
    LOG.debug("Received {}", TaskMessage.GSON_FOR_TASKS.toJson(decodedMsg));
    return decodedMsg;
  }

  private static final Gson GSON_FOR_TASKS =
      new GsonBuilder().setExclusionStrategies(new GsonIgnoreExclusionStrategy())
          .registerTypeHierarchyAdapter(byte[].class, new ByteArrayToBase64TypeAdapter()).create();

  private String taskId;
  private TaskMessageType type;

  public TaskMessage() {}

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  void setMessageType(TaskMessageType type) {
    this.type = type;
  }

  public TaskMessageType getMessageType() {
    return type;
  }

  public Task toThriftTask() {
    Task t = new Task();
    t.setTaskId(getTaskId());
    t.setMessageType(getMessageType().name());
    t.setMessage(TaskMessage.GSON_FOR_TASKS.toJson(this));
    LOG.debug("Sending {}", TaskMessage.GSON_FOR_TASKS.toJson(this));
    return t;
  }

}
