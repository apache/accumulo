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
package org.apache.accumulo.core.util.compaction;

import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;

import com.google.common.base.Preconditions;

public class CompactionExecutorIdImpl extends CompactionExecutorId {

  protected CompactionExecutorIdImpl(String canonical) {
    super(canonical);
  }

  private static final long serialVersionUID = 1L;

  public boolean isExternalId() {
    return canonical().startsWith("e.");
  }

  public String getExternalName() {
    Preconditions.checkState(isExternalId());
    return canonical().substring("e.".length());
  }

  public static CompactionExecutorId internalId(CompactionServiceId csid, String executorName) {
    return new CompactionExecutorIdImpl("i." + csid + "." + executorName);
  }

  public static CompactionExecutorId externalId(String executorName) {
    return new CompactionExecutorIdImpl("e." + executorName);
  }

}
