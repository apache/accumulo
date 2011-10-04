/**
 * 
 */
package org.apache.accumulo.server.test.randomwalk;

/**
 * Tests are extended by users to perform actions on
 * accumulo and are a node of the graph
 */
public abstract class Test extends Node {
	
	@Override
	public String toString() {
		return getClass().getName();
	}
}
