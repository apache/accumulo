package org.apache.accumulo.examples.dirlist;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.aggregation.Aggregator;
import org.apache.accumulo.core.iterators.aggregation.NumSummation;


public class StringArraySummation implements Aggregator {
	List<Long> sums = new ArrayList<Long>();

	@Override
	public void reset() {
		sums.clear();
	}

	@Override
	public void collect(Value value) {
		String[] longs = value.toString().split(",");
		int i;
		for (i=0; i < sums.size() && i < longs.length; i++) {
			sums.set(i, NumSummation.safeAdd(sums.get(i).longValue(), Long.parseLong(longs[i])));
		}
		for (; i < longs.length; i++) {
			sums.add(Long.parseLong(longs[i]));
		}
	}

	@Override
	public Value aggregate() {
		return new Value(longArrayToStringBytes(sums.toArray(new Long[sums.size()])));
	}

	public static byte[] longArrayToStringBytes(Long[] l) {
		if (l.length==0) return new byte[]{};
		StringBuilder sb = new StringBuilder(Long.toString(l[0]));
		for (int i=1; i < l.length; i++) {
			sb.append(",");
			sb.append(Long.toString(l[i]));
		}
		return sb.toString().getBytes();
	}
}
