package org.apache.accumulo.server.util;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.format.DefaultFormatter;


public class PrintTable
{
	public static void main(String[] args)
	throws AccumuloException, AccumuloSecurityException, TableNotFoundException
	{
		if (args.length!=3)
		{
			System.out.println("Usage : PrintTable <table> <user> <password>");
			return;
		}
		
		Connector connector = HdfsZooInstance.getInstance().getConnector(args[1], args[2].getBytes());
		Authorizations auths = connector.securityOperations().getUserAuthorizations(args[1]);
		Scanner scanner = connector.createScanner(args[0], auths);
		
		for (Entry<Key, Value> entry : scanner)
			System.out.println(DefaultFormatter.formatEntry(entry, true));
	}
}
