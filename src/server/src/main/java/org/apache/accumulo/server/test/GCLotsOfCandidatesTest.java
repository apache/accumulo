package org.apache.accumulo.server.test;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.io.Text;


public class GCLotsOfCandidatesTest
{
	public static void main(String args[])
	throws AccumuloException, AccumuloSecurityException, TableNotFoundException, MutationsRejectedException
	{
		if (args.length != 4)
			throw new IllegalArgumentException("Expected arguments: <instance name> <zookeeper server> <username> <password>");
		
		Connector conn = new ZooKeeperInstance(args[0], args[1]).getConnector(args[2], args[3].getBytes());
		generateCandidates(conn);
	}
	
	private static void generateCandidates(Connector conn)
	throws AccumuloException, AccumuloSecurityException, TableNotFoundException, MutationsRejectedException
	{
		conn.securityOperations().grantTablePermission(conn.whoami(), Constants.METADATA_TABLE_NAME, TablePermission.WRITE);
		BatchWriter bw = conn.createBatchWriter(Constants.METADATA_TABLE_NAME, 1L<<20, 0l, 2);
		
		for (int i = 0; i < 10000; ++i)
		{
			final Text emptyText = new Text("");
			Text row = new Text(String.format("%s%s%020d%s", Constants.METADATA_DELETE_FLAG_PREFIX, "/", i, "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjj"));
			Mutation delFlag = new Mutation(row);
			delFlag.put(emptyText, emptyText, new Value(new byte[]{}));
			bw.addMutation(delFlag);
		}
		bw.close();
	}
}
