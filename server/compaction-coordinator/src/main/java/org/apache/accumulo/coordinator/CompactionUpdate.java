/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.coordinator;

import org.apache.accumulo.core.compaction.thrift.TCompactionState;

public class CompactionUpdate {

  private final Long timestamp;
  private final String message;
  private final TCompactionState state;

  CompactionUpdate(Long timestamp, String message, TCompactionState state) {
    super();
    this.timestamp = timestamp;
    this.message = message;
    this.state = state;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public String getMessage() {
    return message;
  }

  public TCompactionState getState() {
    return state;
  }

}
