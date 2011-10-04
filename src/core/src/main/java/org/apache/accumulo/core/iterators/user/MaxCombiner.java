package org.apache.accumulo.core.iterators.user;

import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.LongCombiner;


public class MaxCombiner extends LongCombiner {
	@Override
	public Long typedReduce(Key key, Iterator<Long> iter) {
		long max = Long.MIN_VALUE;
		while (iter.hasNext()) {
			Long l = iter.next();
			if (l > max) max = l;
		}
		return max;
	}
}
