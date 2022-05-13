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

import org.apache.accumulo.start.spi.KeywordExecutable;

import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class CoordinatorExecutable implements KeywordExecutable {

  public static final String EXE_NAME = "compaction-coordinator";

  @Override
  public String keyword() {
    return EXE_NAME;
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroup.PROCESS;
  }

  @Override
  public String description() {
    return "Starts Accumulo Compaction Coordinator";
  }

  @Override
  public void execute(final String[] args) throws Exception {
    System.err.println("WARNING: External compaction processes are experimental");
    CompactionCoordinator.main(args);
  }

}
