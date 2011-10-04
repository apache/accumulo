package org.apache.accumulo.core.util.shell.commands;

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;


public class MasterStateCommand extends Command {

    @Override
    public String description() {
        return "DEPRECATED: use the command line utility instead";
    }

    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
        shellState.getReader().printString("This command is no longer supported in the shell, please use \n");
        shellState.getReader().printString("  accumulo accumulo.server.master.state.SetGoalState [NORMAL|SAFE_MODE|CLEAN_STOP]\n");
        return -1;
    }
    
    @Override
    public String usage() {
        return "use the command line utility instead";
    }

    @Override
    public int numArgs() {
        return 0;
    }
    
}
