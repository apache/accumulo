 package org.apache.accumulo.core.client.impl;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.thrift.TApplicationException;


/**
 * This class is intended to encapsulate errors
 * that occurred on the server side.
 * 
 */

public class AccumuloServerException extends AccumuloException {
	private static final long serialVersionUID = 1L;
	private String server;

	public AccumuloServerException(String server, TApplicationException tae){
		super("Error on server "+server, tae);
		this.setServer(server);
	}

	private void setServer(String server) {
		this.server = server;
	}

	public String getServer() {
		return server;
	}
	
	
}

