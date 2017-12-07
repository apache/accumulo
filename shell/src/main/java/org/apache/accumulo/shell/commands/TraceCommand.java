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

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.tracer.TraceDump;
import org.apache.accumulo.tracer.TraceDump.Printer;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.io.Text;

public class TraceCommand extends DebugCommand {

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws IOException {
    if (cl.getArgs().length == 1) {
      if (cl.getArgs()[0].equalsIgnoreCase("on")) {
        Trace.on("shell:" + shellState.getPrincipal());
      } else if (cl.getArgs()[0].equalsIgnoreCase("off")) {
        if (Trace.isTracing()) {
          final long trace = Trace.currentTraceId();
          Trace.off();
          StringBuilder sb = new StringBuilder();
          int traceCount = 0;
          for (int i = 0; i < 30; i++) {
            sb = new StringBuilder();
            try {
              final Map<String,String> properties = shellState.getConnector().instanceOperations().getSystemConfiguration();
              final String table = properties.get(Property.TRACE_TABLE.getKey());
              final String user = shellState.getConnector().whoami();
              final Authorizations auths = shellState.getConnector().securityOperations().getUserAuthorizations(user);
              final Scanner scanner = shellState.getConnector().createScanner(table, auths);
              scanner.setRange(new Range(new Text(Long.toHexString(trace))));
              final StringBuilder finalSB = sb;
              traceCount = TraceDump.printTrace(scanner, new Printer() {
                @Override
                public void print(final String line) {
                  try {
                    finalSB.append(line + "\n");
                  } catch (Exception ex) {
                    throw new RuntimeException(ex);
                  }
                }
              });
              if (traceCount > 0) {
                shellState.getReader().print(sb.toString());
                break;
              }
            } catch (Exception ex) {
              shellState.printException(ex);
            }
            shellState.getReader().println("Waiting for trace information");
            shellState.getReader().flush();
            sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
          }
          if (traceCount < 0) {
            // display the trace even though there are unrooted spans
            shellState.getReader().print(sb.toString());
          }
        } else {
          shellState.getReader().println("Not tracing");
        }
      } else
        throw new BadArgumentException("Argument must be 'on' or 'off'", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
    } else if (cl.getArgs().length == 0) {
      shellState.getReader().println(Trace.isTracing() ? "on" : "off");
    } else {
      shellState.printException(new IllegalArgumentException("Expected 0 or 1 argument. There were " + cl.getArgs().length + "."));
      printHelp(shellState);
      return 1;
    }
    return 0;
  }

  @Override
  public String description() {
    return "turns trace logging on or off";
  }
}
