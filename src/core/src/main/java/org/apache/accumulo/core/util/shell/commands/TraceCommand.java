package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.TraceDump;
import org.apache.accumulo.core.trace.TraceDump.Printer;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.io.Text;

import cloudtrace.instrument.Trace;

public class TraceCommand extends DebugCommand {

    public int execute(String fullCommand, CommandLine cl, final Shell shellState) throws IOException {
        if (cl.getArgs().length == 1) {
            if (cl.getArgs()[0].equalsIgnoreCase("on")) {
                Trace.on("shell:" + shellState.getCredentials().user);
            } else if (cl.getArgs()[0].equalsIgnoreCase("off")) {
                if (Trace.isTracing()) {
                    long trace = Trace.currentTrace().traceId();
                    Trace.off();
                    for (int i = 0; i < 10; i++) {
                        try {
                            Map<String, String> properties = shellState.getConnector().instanceOperations().getSystemConfiguration();
                            String table = properties.get(Property.TRACE_TABLE.getKey());
                            String user = shellState.getConnector().whoami();
                            Authorizations auths = shellState.getConnector().securityOperations().getUserAuthorizations(user);
                            Scanner scanner = shellState.getConnector().createScanner(table, auths);
                            scanner.setRange(new Range(new Text(Long.toHexString(trace))));
                            final StringBuffer sb = new StringBuffer();
                            if (TraceDump.printTrace(scanner, new Printer() {
                                @Override
                                public void print(String line) {
                                    try {
                                        sb.append(line + "\n");
                                    } catch (Exception ex) {
                                        throw new RuntimeException(ex);
                                    }
                                } }) > 0) {
                                shellState.getReader().printString(sb.toString());
                                break;
                            }
                        } catch (Exception ex) {
                            Shell.printException(ex);
                        }
                        shellState.getReader().printString("Waiting for trace information\n");
                        shellState.getReader().flushConsole();
                        UtilWaitThread.sleep(500);
                    }
                } else {
                    shellState.getReader().printString("Not tracing\n");
                }
            }
            else
                throw new BadArgumentException("Argument must be 'on' or 'off'", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
        } else if (cl.getArgs().length == 0) {
            shellState.getReader().printString(Trace.isTracing() ? "on\n" : "off\n");
        } else {
            Shell.printException(new IllegalArgumentException("Expected 0 or 1 argument. There were " + cl.getArgs().length + "."));
            printHelp();
            return 1;
        }
        return 0;
    }

    @Override
    public String description() {
        return "turns trace logging on or off";
    }
}