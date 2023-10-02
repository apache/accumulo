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

import java.lang.reflect.InvocationTargetException;

import org.apache.accumulo.core.tasks.compaction.ActiveCompactionTasks;
import org.apache.accumulo.core.tasks.compaction.CompactionTask;
import org.apache.accumulo.core.tasks.compaction.CompactionTaskCompleted;
import org.apache.accumulo.core.tasks.compaction.CompactionTaskFailed;
import org.apache.accumulo.core.tasks.compaction.CompactionTaskStatus;
import org.apache.accumulo.core.tasks.compaction.CompactionTasksCompleted;
import org.apache.accumulo.core.tasks.compaction.CompactionTasksRunning;

public enum TaskMessageType {

  // Compaction Related Tasks
  COMPACTION_TASK(CompactionTask.class),
  COMPACTION_TASK_COMPLETED(CompactionTaskCompleted.class),
  COMPACTION_TASK_FAILED(CompactionTaskFailed.class),
  COMPACTION_TASK_LIST(ActiveCompactionTasks.class),
  COMPACTION_TASK_STATUS(CompactionTaskStatus.class),
  COMPACTION_TASKS_COMPLETED(CompactionTasksCompleted.class),
  COMPACTION_TASKS_RUNNING(CompactionTasksRunning.class);

  // TODO: Tasks for calculating split points

  // TODO: Tasks for log sorting and recovery

  private Class<? extends TaskMessage> taskClass;

  TaskMessageType(Class<? extends TaskMessage> taskClass) {
    this.taskClass = taskClass;
  }

  public Class<? extends TaskMessage> getTaskClass() {
    return this.taskClass;
  }

  @SuppressWarnings("unchecked")
  public <T extends TaskMessage> T getTaskMessage() {
    try {
      T msg = (T) this.getTaskClass().getConstructor().newInstance();
      msg.setMessageType(this);
      return msg;
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException | NoSuchMethodException | SecurityException e) {
      throw new RuntimeException("Error creating instance of " + taskClass);
    }
  }

}
