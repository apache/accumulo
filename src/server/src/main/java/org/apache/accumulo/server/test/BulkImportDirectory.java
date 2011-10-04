package org.apache.accumulo.server.test;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;


public class BulkImportDirectory {
	public static void main(String[] args) throws IOException, AccumuloException, AccumuloSecurityException {
		if (args.length != 5)
			throw new RuntimeException("Usage: bin/accumulo " + BulkImportDirectory.class.getName() + " <username> <password> <tablename> <sourcedir> <failuredir>");

		String user = args[0];
		byte[] pass = args[1].getBytes();
		String tableName = args[2];
		String dir = args[3];
		String failureDir = args[4];

		int numMapThreads = 4;
		int numAssignThreads = 20;
		boolean disableGC = false;

		HdfsZooInstance.getInstance().getConnector(user, pass).tableOperations().importDirectory(tableName, dir, failureDir, numMapThreads, numAssignThreads, disableGC);
	}
}
