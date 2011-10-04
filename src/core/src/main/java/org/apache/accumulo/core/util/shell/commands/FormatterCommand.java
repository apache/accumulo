package org.apache.accumulo.core.util.shell.commands;

import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;


public class FormatterCommand extends Command {
	private Option resetOption, formatterClassOption, listClassOption;

	@Override
	public String description() {
		return "specifies a formatter to use for displaying database entries";
	}

	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
		if (cl.hasOption(resetOption.getOpt()))
			shellState.setFormatterClass(DefaultFormatter.class);
		else if (cl.hasOption(formatterClassOption.getOpt()))
			shellState.setFormatterClass(AccumuloClassLoader.loadClass(cl.getOptionValue(formatterClassOption.getOpt()), Formatter.class));
		else if (cl.hasOption(listClassOption.getOpt()))
			shellState.getReader().printString(shellState.getFormatterClass().getName() + "\n");
		return 0;
	}

	@Override
	public Options getOptions() {
		Options o = new Options();
		OptionGroup formatGroup = new OptionGroup();

		resetOption = new Option("r", "reset", false, "reset to default formatter");
		formatterClassOption = new Option("f", "formatter", true, "fully qualified name of formatter class to use");
		formatterClassOption.setArgName("className");
		listClassOption = new Option("l", "list", false, "display the current formatter");
		formatGroup.addOption(resetOption);
		formatGroup.addOption(formatterClassOption);
		formatGroup.addOption(listClassOption);
		formatGroup.setRequired(true);

		o.addOptionGroup(formatGroup);

		return o;
	}

	@Override
	public int numArgs() {
		return 0;
	}

}