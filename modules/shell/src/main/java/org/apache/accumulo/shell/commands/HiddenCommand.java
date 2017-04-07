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
package org.apache.accumulo.shell.commands;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.Random;

import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellCommandException;
import org.apache.accumulo.shell.ShellCommandException.ErrorCode;
import org.apache.commons.cli.CommandLine;

public class HiddenCommand extends Command {
  private static Random rand = new SecureRandom();

  @Override
  public String description() {
    return "The first rule of Accumulo is: \"You don't talk about Accumulo.\"";
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {
    if (rand.nextInt(10) == 0) {
      shellState.getReader().beep();
      shellState.getReader().println();
      shellState.getReader().println(
          new String(Base64.getDecoder().decode(
              "ICAgICAgIC4tLS4KICAgICAgLyAvXCBcCiAgICAgKCAvLS1cICkKICAgICAuPl8gIF88LgogICAgLyB8ICd8ICcgXAog"
                  + "ICAvICB8Xy58Xy4gIFwKICAvIC98ICAgICAgfFwgXAogfCB8IHwgfFwvfCB8IHwgfAogfF98IHwgfCAgfCB8IHxffAogICAgIC8gIF9fICBcCiAgICAvICAv"
                  + "ICBcICBcCiAgIC8gIC8gICAgXCAgXF8KIHwvICAvICAgICAgXCB8IHwKIHxfXy8gICAgICAgIFx8X3wK"), UTF_8));
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
