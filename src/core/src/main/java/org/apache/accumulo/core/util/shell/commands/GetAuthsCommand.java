package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


public class GetAuthsCommand extends Command {
	private Option userOpt;

	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
		String user = cl.getOptionValue(userOpt.getOpt(), shellState.getConnector().whoami());
		shellState.getReader().printString(shellState.getConnector().securityOperations().getUserAuthorizations(user) + "\n");
		return 0;
	}

	@Override
	public String description() {
		return "displays the maximum scan authorizations for a user";
	}

	@Override
	public Options getOptions() {
		Options o = new Options();
		userOpt = new Option(Shell.userOption, "user", true, "user to operate on");
		userOpt.setArgName("user");
		o.addOption(userOpt);
		return o;
	}

	@Override
	public int numArgs() {
		return 0;
	}
}