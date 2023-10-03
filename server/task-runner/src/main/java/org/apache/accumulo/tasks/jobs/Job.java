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
package org.apache.accumulo.tasks.jobs;

import org.apache.accumulo.core.tasks.TaskMessage;
import org.apache.accumulo.tasks.TaskRunnerProcess;
import org.apache.thrift.TException;

public abstract class Job<T extends TaskMessage> {

  private final TaskRunnerProcess worker;
  protected final T msg;

  public Job(TaskRunnerProcess worker, T msg) {
    this.worker = worker;
    this.msg = msg;
  }

  public TaskRunnerProcess getTaskWorker() {
    return this.worker;
  }

  public abstract Runnable createJob() throws Exception;

  public abstract void executeJob(Thread executionThread) throws InterruptedException;

  public abstract void cancel(String id) throws TException;
}
