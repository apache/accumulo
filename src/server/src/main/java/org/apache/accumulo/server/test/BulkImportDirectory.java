package org.apache.accumulo.server.test;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class BulkImportDirectory {
	public static void main(String[] args) throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
		if (args.length != 5)
			throw new RuntimeException("Usage: bin/accumulo " + BulkImportDirectory.class.getName() + " <username> <password> <tablename> <sourcedir> <failuredir>");

		final String user = args[0];
		final byte[] pass = args[1].getBytes();
		final String tableName = args[2];
		final String dir = args[3];
		final String failureDir = args[4];
		final Path failureDirPath = new Path(failureDir);
		final FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
		fs.delete(failureDirPath, true);
		fs.mkdirs(failureDirPath);
		HdfsZooInstance.getInstance().getConnector(user, pass).tableOperations().importDirectory(tableName, dir, failureDir, false);
	}
}
