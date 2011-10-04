package org.apache.accumulo.core.iterators.aggregation.conf;

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.aggregation.Aggregator;
import org.apache.accumulo.core.iterators.conf.ColumnToClassMapping;


public class AggregatorSet extends ColumnToClassMapping<Aggregator>{
	public AggregatorSet(Map<String, String> opts) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		super(opts, Aggregator.class);
	}

	public AggregatorSet() {
		super();
	}

	public Aggregator getAggregator(Key k){
		return getObject(k);
	}
}
