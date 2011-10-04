package org.apache.accumulo.core.util.shell.commands;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


public class DeleteIterCommand extends Command {
	private Option tableOpt, mincScopeOpt, majcScopeOpt, scanScopeOpt, nameOpt;

	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		String tableName;
		
		if(cl.hasOption(tableOpt.getOpt())){
			tableName = cl.getOptionValue(tableOpt.getOpt());
			if (!shellState.getConnector().tableOperations().exists(tableName))
				throw new TableNotFoundException(null, tableName, null);
		}
		
		else{
			shellState.checkTableState();
			tableName = shellState.getTableName();
		}
		
		String name = cl.getOptionValue(nameOpt.getOpt());
		if (!shellState.getConnector().tableOperations().getIterators(tableName).contains(name)) {
		    Shell.log.warn("no iterators found that match your criteria");
		    return 0;
		}
		IteratorSetting iterator = shellState.getConnector().tableOperations().getIterator(tableName, name);
		if (iterator == null)
		    return 0;
		boolean deleteAll = true;
		if (cl.hasOption(mincScopeOpt.getOpt())) {
			iterator.deleteOptions(IteratorScope.minc);
			deleteAll = false;
		}
		if (cl.hasOption(majcScopeOpt.getOpt())) {
		    iterator.deleteOptions(IteratorScope.majc);
		    deleteAll = false;
		}
		if (cl.hasOption(scanScopeOpt.getOpt())) {
		    iterator.deleteOptions(IteratorScope.scan);
		    deleteAll = false;
		}
		shellState.getConnector().tableOperations().removeIterator(tableName, name);
		if (!iterator.getProperties().isEmpty() && !deleteAll)
		    shellState.getConnector().tableOperations().attachIterator(tableName, iterator);
		return 0;
	}

	@Override
	public String description() {
		return "deletes a table-specific iterator";
	}

	public Options getOptions() {
		Options o = new Options();

		tableOpt = new Option(Shell.tableOption, "table", true, "tableName");
		tableOpt.setArgName("table");

		nameOpt = new Option("n", "name", true, "iterator to delete");
		nameOpt.setArgName("itername");
		nameOpt.setRequired(true);

		mincScopeOpt = new Option(IteratorScope.minc.name(), "minor-compaction", false, "applied at minor compaction");
		majcScopeOpt = new Option(IteratorScope.majc.name(), "major-compaction", false, "applied at major compaction");
		scanScopeOpt = new Option(IteratorScope.scan.name(), "scan-time", false, "applied at scan time");

		o.addOption(tableOpt);
		o.addOption(nameOpt);

		o.addOption(mincScopeOpt);
		o.addOption(majcScopeOpt);
		o.addOption(scanScopeOpt);

		return o;
	}

	@Override
	public int numArgs() {
		return 0;
	}
}