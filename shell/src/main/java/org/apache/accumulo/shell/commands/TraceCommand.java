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

import java.io.IOException;

import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

public class TraceCommand extends Command {

  private static final String SPAN_FORMAT = "%s::%s";

  private Tracer tracer;
  private Pair<Span,Scope> traceInfo;

  public TraceCommand() {
    disable();
  }

  private synchronized void enable(Shell shellState) {
    if (tracer == null) {
      tracer = GlobalOpenTelemetry.get().getTracer(TraceCommand.class.getSimpleName());
    }
    if (traceInfo == null) {
      String spanName = String.format(SPAN_FORMAT, Shell.class.getSimpleName(),
          shellState.getAccumuloClient().whoami());
      Span span = tracer.spanBuilder(spanName).startSpan();
      Scope scope = span.makeCurrent();
      traceInfo = new Pair<>(span, scope);
    }
  }

  private synchronized void disable() {
    if (traceInfo != null) {
      try {
        traceInfo.getSecond().close();
      } finally {
        traceInfo.getFirst().end();
        traceInfo = null;
      }
    }
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws IOException {
    if (cl.getArgs().length == 1) {
      if (cl.getArgs()[0].equalsIgnoreCase("on")) {
        enable(shellState);
      } else if (cl.getArgs()[0].equalsIgnoreCase("off")) {
        disable();
      } else {
        throw new BadArgumentException("Argument must be 'on' or 'off'", fullCommand,
            fullCommand.indexOf(cl.getArgs()[0]));
      }
    } else if (cl.getArgs().length == 0) {
      shellState.getWriter().println(traceInfo == null ? "off" : "on");
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
