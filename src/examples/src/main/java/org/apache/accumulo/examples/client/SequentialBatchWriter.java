package org.apache.accumulo.examples.client;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;

public class SequentialBatchWriter {
	
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, MutationsRejectedException
	{
		if(args.length != 12)
		{
			System.out.println("Usage : SequentialBatchWriter <instance name> <zoo keepers> <username> <password> <table> <start> <num> <value size> <max memory> <max latency> <num threads> <visibility>");
			return;
		}
		
		String instanceName = args[0];
		String zooKeepers = args[1];
		String user = args[2];
		byte[] pass = args[3].getBytes();
		String table = args[4];
		long start = Long.parseLong(args[5]);
		long num = Long.parseLong(args[6]);
		int valueSize = Integer.parseInt(args[7]);
		long maxMemory = Long.parseLong(args[8]);
		long maxLatency = Long.parseLong(args[9]) == 0 ? Long.MAX_VALUE
				: Long.parseLong(args[9]);
		int numThreads = Integer.parseInt(args[10]);
		String visibility = args[11];
		
		//Uncomment the following lines for detailed debugging info
		//Logger logger = Logger.getLogger(Constants.CORE_PACKAGE_NAME);
		//logger.setLevel(Level.TRACE);
		
		ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
		Connector connector = instance.getConnector(user, pass);
		BatchWriter bw = connector.createBatchWriter(table, maxMemory, maxLatency, numThreads);
		
		long end = start + num;
		
		//reuse ColumnVisibility object for better performance
		ColumnVisibility cv = new ColumnVisibility(visibility);
		
		for(long i = start; i < end; i++)
		{
			Mutation m = RandomBatchWriter.createMutation(i, valueSize, cv);
			bw.addMutation(m);
		}
		
		bw.close();
	}
}
