package org.apache.accumulo.core.iterators.aggregation;

import org.apache.accumulo.core.data.Value;

public class StringSummation implements Aggregator {

	long sum = 0;
	
	public Value aggregate() {
		return new Value(Long.toString(sum).getBytes());
	}

	public void collect(Value value) {
		sum += Long.parseLong(new String(value.get()));
	}

	public void reset() {
		sum = 0;
		
	}
}
