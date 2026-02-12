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
package org.apache.accumulo.server.util.adminCommand;

import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.JCommander;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class Locks extends ServerKeywordExecutable<ServerUtilOpts> {

  public Locks() {
    super(new ServerUtilOpts());
  }

  @Override
  public String keyword() {
    return "locks";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.ADMIN;
  }

  @Override
  public String description() {
    return "Prints server locks (Deprecated - use service-status and stop commands)";
  }

  @Override
  public void execute(JCommander cl, ServerUtilOpts options) throws Exception {
    System.out.println("'locks' command has been removed. Use 'service-status' command"
        + " to list processes and 'stop -f' command to remove their locks.");
  }

}
