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
package org.apache.accumulo.shell.commands;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.SecureRandom;
import java.util.Base64;

import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellCommandException;
import org.apache.accumulo.shell.ShellCommandException.ErrorCode;
import org.apache.commons.cli.CommandLine;
import org.jline.utils.InfoCmp;

public class HiddenCommand extends Command {
  private static final SecureRandom random = new SecureRandom();

  @Override
  public String description() {
    return "The first rule of Accumulo is: \"You don't talk about Accumulo.\"";
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {
    if (random.nextInt(10) == 0) {
      shellState.getTerminal().puts(InfoCmp.Capability.bell);
      shellState.getWriter().println();
      shellState.getWriter()
          .println(new String(Base64.getDecoder()
              .decode("ICAgICAgIC4tLS4KICAgICAgLyAvXCBcCiAgICAgKCAvLS1cICkKICAgICAuPl8g"
                  + "IF88LgogICAgLyB8ICd8ICcgXAogICAvICB8Xy58Xy4gIFwKICAvIC98ICAgIC"
                  + "AgfFwgXAogfCB8IHwgfFwvfCB8IHwgfAogfF98IHwgfCAgfCB8IHxffAogICAg"
                  + "IC8gIF9fICBcCiAgICAvICAvICBcICBcCiAgIC8gIC8gICAgXCAgXF8KIHwvIC"
                  + "AvICAgICAgXCB8IHwKIHxfXy8gICAgICAgIFx8X3wK"),
              UTF_8));
    } else {
      throw new ShellCommandException(ErrorCode.UNRECOGNIZED_COMMAND, getName());
    }
    return 0;
  }

  @Override
  public int numArgs() {
    return Shell.NO_FIXED_ARG_LENGTH_CHECK;
  }

  @Override
  public String getName() {
    return "accvmvlo";
  }
}
