package org.apache.accumulo.server.test.randomwalk.concurrent;

import org.apache.accumulo.server.test.randomwalk.Fixture;
import org.apache.accumulo.server.test.randomwalk.State;

/**
 * When multiple instance of this test suite are run, all instances will operate on the same set of table names.
 * 
 *
 */

public class ConcurrentFixture extends Fixture {

	@Override
	public void setUp(State state) throws Exception {
	}

	@Override
	public void tearDown(State state) throws Exception {
	}

}
