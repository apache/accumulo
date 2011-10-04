package org.apache.accumulo.core.util.shell.commands;

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;


public class SleepCommand extends Command {

    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState)
            throws Exception {
        double secs = Double.parseDouble(cl.getArgs()[0]);
        Thread.sleep((long)(secs * 1000));
        return 0;
    }

    @Override
    public String description() {
        return "sleep for the given number of seconds";
    }

    @Override
    public int numArgs() {
        return 1;
    }
    
}