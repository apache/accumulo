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
package org.apache.accumulo.tasks;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.tasks.thrift.TaskManager;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.CompactionWatcher;
import org.apache.accumulo.server.compaction.PausedCompactionMetrics;
import org.apache.thrift.transport.TTransportException;

// This is the view of the TaskWorker that the Job's get
public interface TaskRunnerProcess {

  ServerContext getContext();

  AccumuloConfiguration getConfiguration();

  String getResourceGroup();

  TaskManager.Client getCoordinatorClient() throws TTransportException;

  CompactionWatcher getCompactionWatcher();

  PausedCompactionMetrics getPausedCompactionMetrics();

  void shutdown();
}
