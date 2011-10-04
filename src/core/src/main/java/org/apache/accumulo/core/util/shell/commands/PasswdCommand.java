package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


public class PasswdCommand extends Command {
	private Option userOpt;

	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
		String currentUser = shellState.getConnector().whoami();
		String user = cl.getOptionValue(userOpt.getOpt(), currentUser);

		String password = null;
		String passwordConfirm = null;
		String oldPassword = null;

		oldPassword = shellState.getReader().readLine("Enter current password for '" + currentUser + "': ", '*');
		if (oldPassword == null) {
			shellState.getReader().printNewline();
			return 0;
		} // user canceled

		if (!shellState.getConnector().securityOperations().authenticateUser(currentUser, oldPassword.getBytes()))
			throw new AccumuloSecurityException(user, SecurityErrorCode.BAD_CREDENTIALS);

		password = shellState.getReader().readLine("Enter new password for '" + user + "': ", '*');
		if (password == null) {
			shellState.getReader().printNewline();
			return 0;
		} // user canceled
		passwordConfirm = shellState.getReader().readLine("Please confirm new password for '" + user + "': ", '*');
		if (passwordConfirm == null) {
			shellState.getReader().printNewline();
			return 0;
		} // user canceled

		if (!password.equals(passwordConfirm))
			throw new IllegalArgumentException("Passwords do not match");

		byte[] pass = password.getBytes();
		shellState.getConnector().securityOperations().changeUserPassword(user, pass);
		// update the current credentials if the password changed was for
		// the current user
		if (shellState.getConnector().whoami().equals(user))
			shellState.updateUser(new AuthInfo(user, ByteBuffer.wrap(pass), shellState.getConnector().getInstance().getInstanceID()));
		Shell.log.debug("Changed password for user " + user);
		return 0;
	}

	@Override
	public String description() {
		return "changes a user's password";
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