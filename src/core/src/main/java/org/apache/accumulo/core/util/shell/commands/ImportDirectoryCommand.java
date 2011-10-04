package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class ImportDirectoryCommand extends Command {

	@Override
	public String description() {
		return "bulk imports an entire directory of data files to the current table.  The boolean argument determines if accumulo sets the time.";
	}

	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
		shellState.checkTableState();

		String dir = cl.getArgs()[0];
		String failureDir = cl.getArgs()[1];
		boolean setTime = Boolean.parseBoolean(cl.getArgs()[2]);
		
		FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
		FileStatus failStatus = fs.getFileStatus(new Path(failureDir));
		if (failStatus == null || !failStatus.isDir() || fs.listStatus(new Path(failureDir)).length != 0)
		    throw new AccumuloException(failureDir + " is not an empty directory");
		shellState.getConnector().tableOperations().importDirectory(shellState.getTableName(), dir, failureDir, setTime);
		return 0;
	}

	@Override
	public int numArgs() {
		return 3;
	}

	@Override
	public String usage() {
		return getName() + " <directory> <failureDirectory> true|false";
	}

}