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
package org.apache.accumulo.server.util.checkCommand;

import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.Admin;

public interface CheckRunner {

  /**
   * Runs the check
   *
   * @param context server context
   * @param opts server util opts. Only applicable for the checks on the root and metadata tables
   * @param fixFiles remove dangling file pointers. Only applicable for the checks on the system and
   *        user files
   * @return the {@link Admin.CheckCommand.CheckStatus} resulting from running the check
   */
  Admin.CheckCommand.CheckStatus runCheck(ServerContext context, ServerUtilOpts opts,
      boolean fixFiles) throws Exception;

  /**
   *
   * @return the check that this check runner runs
   */
  Admin.CheckCommand.Check getCheck();

  default void printRunning() {
    String running = "Running check " + getCheck();
    System.out.println();
    System.out.println("-".repeat(running.length()));
    System.out.println(running);
    System.out.println("-".repeat(running.length()));
  }

  default void printCompleted(Admin.CheckCommand.CheckStatus status) {
    String completed = "Check " + getCheck() + " completed with status " + status;
    System.out.println();
    System.out.println("-".repeat(completed.length()));
    System.out.println(completed);
    System.out.println("-".repeat(completed.length()));
  }
}
