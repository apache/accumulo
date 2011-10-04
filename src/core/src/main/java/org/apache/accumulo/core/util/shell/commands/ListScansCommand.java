package org.apache.accumulo.core.util.shell.commands;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


public class ListScansCommand extends Command {

	private Option tserverOption, disablePaginationOpt;
	
	@Override
	public String description() {
		return "list what scans are currently running in accumulo. See the accumulo.core.client.admin.ActiveScan javadoc for more information about columns.";
	}

	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
		
		List<String> tservers;
		
		InstanceOperations instanceOps = shellState.getConnector().instanceOperations();
		
		boolean paginate = !cl.hasOption(disablePaginationOpt.getOpt());
		
		if(cl.hasOption(tserverOption.getOpt())){
			tservers = new ArrayList<String>();
			tservers.add(cl.getOptionValue(tserverOption.getOpt()));
		}else{
			tservers = instanceOps.getTabletServers();
		}
		
		shellState.printLines(new ActiveScanIterator(tservers, instanceOps), paginate);
		
		return 0;
	}

	@Override
	public int numArgs() {
		return 0;
	}
	
	@Override
	public Options getOptions() {
		Options opts = new Options();

		tserverOption = new Option("ts", "tabletServer", true, "list scans for a specific tablet server");
		tserverOption.setArgName("tablet server");
		opts.addOption(tserverOption);

		disablePaginationOpt = new Option("np", "no-pagination", false, "disables pagination of output");
		opts.addOption(disablePaginationOpt);
		
		return opts;
	}
	
}