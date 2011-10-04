package org.apache.accumulo.examples.client;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;

/**
 * Simple example for using tableOperations() (like create, delete, flush, etc).
 */
public class Flush {

	public static void main(String[] args) {
		if (args.length != 5)
		{
		    System.err.println("Usage: accumulo accumulo.examples.client.Flush <instance name> <zoo keepers> <username> <password> <tableName>");
			return;
		}
		String instanceName = args[0];
        String zooKeepers = args[1];
        String user = args[2];
        String password = args[3];
        String table = args[4];
	
		Connector connector;
		try {
			ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
			connector = instance.getConnector(user, password.getBytes());
			connector.tableOperations().flush(table, null, null, true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
