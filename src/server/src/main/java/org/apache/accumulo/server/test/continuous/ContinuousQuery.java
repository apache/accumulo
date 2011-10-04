package org.apache.accumulo.server.test.continuous;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;



public class ContinuousQuery {

	
	public static void main(String[] args) throws Exception {
		if(args.length != 8){
			throw new IllegalArgumentException("usage : "+ContinuousIngest.class.getName()+" <instance name> <zookeepers> <user> <pass> <table> <min> <max> <sleep time>");
		}
		
		String instanceName = args[0];
		String zooKeepers = args[1];
		
		String user = args[2];
		String password = args[3];
		
		String table = args[4];
		
		long min = Long.parseLong(args[5]);
		long max = Long.parseLong(args[6]);
		
		long sleepTime = Long.parseLong(args[7]);
		
		Connector conn = new ZooKeeperInstance(instanceName, zooKeepers).getConnector(user, password.getBytes());
		Scanner scanner = conn.createScanner(table, new Authorizations());
		
		Random r = new Random();
		
		while(true){
			byte[] row = ContinuousIngest.genRow(min, max, r);
			
			int count = 0;
			
			long t1 = System.currentTimeMillis();
			scanner.setRange(new Range(new Text(row)));
			for (@SuppressWarnings("unused") Entry<Key, Value> entry : scanner) {
				count++;
			}
			long t2 = System.currentTimeMillis();
			
			System.out.printf("SRQ %d %s %d %d\n",t1, new String(row), (t2 - t1), count);
			
			if(sleepTime > 0)
				Thread.sleep(sleepTime);
			
		}
		
	}

}
