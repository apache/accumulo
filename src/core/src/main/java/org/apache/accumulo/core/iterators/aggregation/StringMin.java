package org.apache.accumulo.core.iterators.aggregation;

import org.apache.accumulo.core.data.Value;

public class StringMin implements Aggregator {

	long min = Long.MAX_VALUE;
	
	public Value aggregate() {
		return new Value(Long.toString(min).getBytes());
	}

	public void collect(Value value) {
		long l = Long.parseLong(new String(value.get()));
		if(l < min) {
			min = l;
		}
	}

	public void reset() {
		min = Long.MAX_VALUE;
	}
	
}
