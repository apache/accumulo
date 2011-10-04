package org.apache.accumulo.server.test.randomwalk;

import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * Represents a point in graph of RandomFramework
 */
public abstract class Node {
	
	protected final Logger log = Logger.getLogger(this.getClass());
	
	/**
	 * Visits node
	 * @param state Random walk state passed between nodes
	 * @throws Exception 
	 */
	public abstract void visit(State state, Properties props) throws Exception;
	
	@Override
	public boolean equals(Object o) {
		return toString().equals(o.toString());
	}
	
	@Override
	public int hashCode() {
		return toString().hashCode();
	}
}