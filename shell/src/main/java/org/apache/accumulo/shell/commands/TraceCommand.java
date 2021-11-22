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
package org.apache.accumulo.shell.commands;

import java.io.IOException;

import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class TraceCommand extends Command {

  private Pair<Span,Scope> span;

  public TraceCommand() {
    // off by default
    TraceUtil.disable();
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws IOException {
    if (cl.getArgs().length == 1) {
      if (cl.getArgs()[0].equalsIgnoreCase("on")) {
        TraceUtil.enable();
        if (span == null) {
          var spanOT = TraceUtil.startSpan(Shell.class, shellState.getAccumuloClient().whoami());
          var scopeOT = spanOT.makeCurrent();
          span = new Pair<>(spanOT, scopeOT);
        }
      } else if (cl.getArgs()[0].equalsIgnoreCase("off")) {
        if (span != null) {
          try {
            TraceUtil.disable();
            span.getSecond().close();
          } finally {
            span.getFirst().end();
            span = null;
          }
        } else {
          shellState.getWriter().println("Not tracing");
        }
      } else {
        throw new BadArgumentException("Argument must be 'on' or 'off'", fullCommand,
            fullCommand.indexOf(cl.getArgs()[0]));
      }
    } else if (cl.getArgs().length == 0) {
      shellState.getWriter().println(span == null ? "off" : "on");
    } else {
      shellState.printException(new IllegalArgumentException(
          "Expected 0 or 1 argument. There were " + cl.getArgs().length + "."));
      printHelp(shellState);
      return 1;
    }
    return 0;
  }

  @Override
  public String description() {
    return "turns tracing on or off";
  }

  @Override
  public String usage() {
    return getName() + " [ on | off ]";
  }

  @Override
  public int numArgs() {
    return Shell.NO_FIXED_ARG_LENGTH_CHECK;
  }

}
