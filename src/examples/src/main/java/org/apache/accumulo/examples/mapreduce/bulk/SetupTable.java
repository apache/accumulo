package org.apache.accumulo.examples.mapreduce.bulk;

import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.hadoop.io.Text;


public class SetupTable {
	
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException
	{
		if (args.length == 5)
		{
			// create a basic table
			new ZooKeeperInstance(args[0], args[1]).getConnector(args[2], args[3].getBytes()).tableOperations().create(args[4]);
		}
		else if (args.length > 5)
		{
			// create a table with initial partitions
			Connector conn = new ZooKeeperInstance(args[0], args[1]).getConnector(args[2], args[3].getBytes());
			
			TreeSet<Text> intialPartitions = new TreeSet<Text>();
			for (int i = 5; i < args.length; ++i)
				intialPartitions.add(new Text(args[i]));
		
			conn.tableOperations().create(args[4]);
			conn.tableOperations().addSplits(args[4], intialPartitions);
		}
		else
		{
			System.err.println("Usage : SetupTable <master> <username> <password> <table name> [<split point>{ <split point}]");
		}
	}
}
