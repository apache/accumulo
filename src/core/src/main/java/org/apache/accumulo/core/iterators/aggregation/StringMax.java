package org.apache.accumulo.core.iterators.aggregation;

import org.apache.accumulo.core.data.Value;

public class StringMax implements Aggregator {

	long max = Long.MIN_VALUE;
	
	public Value aggregate() {
		return new Value(Long.toString(max).getBytes());
	}

	public void collect(Value value) {
		long l = Long.parseLong(new String(value.get()));
		if(l > max) {
			max = l;
		}
	}

	public void reset() {
		max = Long.MIN_VALUE;
	}
	
}
