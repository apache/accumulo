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
package org.apache.accumulo.tserver.compactions;

import java.util.function.Consumer;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;

/**
 * A non-pluggable component that executes compactions using multiple threads and has a priority
 * queue. There are two types: Internal and External. The {@link InternalCompactionExecutor} runs
 * within the Accumulo tserver process. The {@link ExternalCompactionExecutor} runs compactions
 * outside the tserver.
 */
public interface CompactionExecutor {

  SubmittedJob submit(CompactionServiceId csid, CompactionJob job, Compactable compactable,
      Consumer<Compactable> completionCallback);

  enum CType {
    INTERNAL, EXTERNAL
  }

  int getCompactionsRunning(CType ctype);

  int getCompactionsQueued(CType ctype);

  void stop();

  void compactableClosed(KeyExtent extent);
}
