package org.apache.accumulo.core.util.shell.commands;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Token;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;


public class SetAuthsCommand extends Command {
	private Option userOpt;
	private Option scanOptAuths;
	private Option clearOptAuths;

	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException {
		String user = cl.getOptionValue(userOpt.getOpt(), shellState.getConnector().whoami());
		String scanOpts = cl.hasOption(clearOptAuths.getOpt()) ? null : cl.getOptionValue(scanOptAuths.getOpt());
		shellState.getConnector().securityOperations().changeUserAuthorizations(user, CreateUserCommand.parseAuthorizations(scanOpts));
		Shell.log.debug("Changed record-level authorizations for user " + user);
		return 0;
	}

	@Override
	public String description() {
		return "sets the maximum scan authorizations for a user";
	}

	@Override
	public void registerCompletion(Token root, Map<Command.CompletionSet, Set<String>> completionSet) {
		registerCompletionForUsers(root, completionSet);
	}

	@Override
	public Options getOptions() {
		Options o = new Options();
		OptionGroup setOrClear = new OptionGroup();
		scanOptAuths = new Option("s", "scan-authorizations", true, "set the scan authorizations");
		scanOptAuths.setArgName("comma-separated-authorizations");
		setOrClear.addOption(scanOptAuths);
		clearOptAuths = new Option("c", "clear-authorizations", false, "clears the scan authorizations");
		setOrClear.addOption(clearOptAuths);
		setOrClear.setRequired(true);
		o.addOptionGroup(setOrClear);
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