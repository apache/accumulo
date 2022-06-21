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
package org.apache.accumulo.manager;

import org.apache.accumulo.start.spi.KeywordExecutable;

import com.google.auto.service.AutoService;

@Deprecated(since = "2.1.0")
@AutoService(KeywordExecutable.class)
public class MasterExecutable implements KeywordExecutable {

  @Override
  public String keyword() {
    return "master";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroup.PROCESS;
  }

  @Override
  public String description() {
    return "Starts Accumulo master (Deprecated)";
  }

  @Override
  public void execute(final String[] args) throws Exception {
    Manager.main(args);
  }

}
