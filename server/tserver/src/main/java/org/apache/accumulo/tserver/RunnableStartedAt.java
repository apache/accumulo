/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.tserver;

import java.util.AbstractMap;
import java.util.Map.Entry;

/**
 * Encapsulation of a task and the time it began execution.
 */
public class RunnableStartedAt extends AbstractMap.SimpleEntry<ActiveAssignmentRunnable,Long> {

  private static final long serialVersionUID = 1L;

  public RunnableStartedAt(ActiveAssignmentRunnable task, Long startedAtMillis) {
    super(task, startedAtMillis);
  }

  public RunnableStartedAt(Entry<? extends ActiveAssignmentRunnable,? extends Long> entry) {
    super(entry);
  }

  /**
   * @return The task being executed
   */
  public ActiveAssignmentRunnable getTask() {
    return getKey();
  }

  /**
   * @return The time, in millis, that the runnable was submitted at
   */
  public Long getStartTime() {
    return getValue();
  }

}
