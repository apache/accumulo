package org.apache.accumulo.core.iterators.user;

import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.LongCombiner;


public class SummingCombiner extends LongCombiner {
	@Override
	public Long typedReduce(Key key, Iterator<Long> iter) {
		long sum = 0;
		while (iter.hasNext()) {
			sum = safeAdd(sum, iter.next());
		}
		return sum;
	}
}
