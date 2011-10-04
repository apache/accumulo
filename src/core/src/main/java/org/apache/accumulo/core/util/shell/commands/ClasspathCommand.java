package org.apache.accumulo.core.util.shell.commands;

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.commons.cli.CommandLine;


public class ClasspathCommand extends Command {
	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) {
		AccumuloClassLoader.printClassPath();
		return 0;
	}

	@Override
	public String description() {
		return "lists the current files on the classpath";
	}

	@Override
	public int numArgs() {
		return 0;
	}
}