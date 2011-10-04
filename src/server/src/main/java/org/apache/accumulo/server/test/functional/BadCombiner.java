package org.apache.accumulo.server.test.functional;

import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;


public class BadCombiner extends Combiner {

	@Override
	public Value reduce(Key key, Iterator<Value> iter) {
        throw new IllegalStateException();
	}

}
