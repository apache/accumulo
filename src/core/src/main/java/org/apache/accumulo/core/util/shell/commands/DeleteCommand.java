package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;


public class DeleteCommand extends Command {
	private Option deleteOptAuths, timestampOpt;

	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException, ConstraintViolationException {
		shellState.checkTableState();

		Mutation m = new Mutation(new Text(cl.getArgs()[0]));

		if (cl.hasOption(deleteOptAuths.getOpt())) {
			ColumnVisibility le = new ColumnVisibility(cl.getOptionValue(deleteOptAuths.getOpt()));
			if(cl.hasOption(timestampOpt.getOpt()))
				m.putDelete(new Text(cl.getArgs()[1]), new Text(cl.getArgs()[2]), le, Long.parseLong(cl.getOptionValue(timestampOpt.getOpt())));
			else
				m.putDelete(new Text(cl.getArgs()[1]), new Text(cl.getArgs()[2]), le);
		} else
			if(cl.hasOption(timestampOpt.getOpt()))
				m.putDelete(new Text(cl.getArgs()[1]), new Text(cl.getArgs()[2]), Long.parseLong(cl.getOptionValue(timestampOpt.getOpt())));
			else
				m.putDelete(new Text(cl.getArgs()[1]), new Text(cl.getArgs()[2]));

		BatchWriter bw = shellState.getConnector().createBatchWriter(shellState.getTableName(), m.estimatedMemoryUsed()+0L, 0L, 1);
		bw.addMutation(m);
		bw.close();
		return 0;
	}

	@Override
	public String description() {
		return "deletes a record from a table";
	}

	@Override
	public String usage() {
		return getName() + " <row> <colfamily> <colqualifier>";
	}

	@Override
	public Options getOptions() {
		Options o = new Options();

		deleteOptAuths = new Option("l", "authorization-label", true, "formatted authorization label expression");
		deleteOptAuths.setArgName("expression");
		o.addOption(deleteOptAuths);

		timestampOpt = new Option("t", "timestamp", true, "timestamp to use for insert");
		timestampOpt.setArgName("timestamp");
		o.addOption(timestampOpt);
		
		return o;
	}

	@Override
	public int numArgs() {
		return 3;
	}
}