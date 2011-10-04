package org.apache.accumulo.examples.aggregation;

import java.util.TreeSet;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.aggregation.Aggregator;
import org.apache.accumulo.core.util.StringUtil;


public class SortedSetAggregator implements Aggregator
{

	TreeSet<String> items = new TreeSet<String>();

	// aggregate the entire set of items, in sorted order
	public Value aggregate()
	{
		return new Value(StringUtil.join(items, ",").getBytes());
	}

	// allow addition of multiple items at a time to the set
	public void collect(Value value)
	{
		String[] strings = value.toString().split(",");
		for (String s : strings)
			items.add(s);
	}

	public void reset()
	{
		items.clear();
	}

}
