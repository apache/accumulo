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
package org.apache.accumulo.server.util;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;

public abstract class ServerProcessKeywordExecutable implements KeywordExecutable {

  private final String keyword;
  private final String name;

  public ServerProcessKeywordExecutable(String keyword, String name) {
    this.keyword = keyword;
    this.name = name;
  }

  @Override
  public String keyword() {
    return keyword;
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.PROCESS;
  }

  @Override
  public String description() {
    return "Starts Accumulo " + name;
  }

  @Override
  public Object getOptions() {
    return new ServerOpts();
  }

}
