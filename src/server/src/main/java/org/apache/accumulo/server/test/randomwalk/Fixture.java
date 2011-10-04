package org.apache.accumulo.server.test.randomwalk;

import org.apache.log4j.Logger;

public abstract class Fixture {
	
	protected final Logger log = Logger.getLogger(this.getClass());
	
	public abstract void setUp(State state) throws Exception;

	public abstract void tearDown(State state) throws Exception;
}
