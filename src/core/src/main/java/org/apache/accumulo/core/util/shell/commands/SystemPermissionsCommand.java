package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;

import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;


public class SystemPermissionsCommand extends Command {
	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws IOException {
		for (String p : SystemPermission.printableValues())
			shellState.getReader().printString(p + "\n");
		return 0;
	}

	@Override
	public String description() {
		return "displays a list of valid system permissions";
	}

	@Override
	public int numArgs() {
		return 0;
	}
}