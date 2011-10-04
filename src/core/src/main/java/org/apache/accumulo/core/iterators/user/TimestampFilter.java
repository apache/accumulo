package org.apache.accumulo.core.iterators.user;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;


public class TimestampFilter extends Filter implements OptionDescriber {
	private final SimpleDateFormat dateParser = new SimpleDateFormat("yyyyMMddHHmmssz");

	public static final String START = "start";
	public static final String START_INCL = "startInclusive";
	public static final String END = "end";
	public static final String END_INCL = "endInclusive";
	private long start;
	private long end;
	private boolean startInclusive;
	private boolean endInclusive;

	public TimestampFilter() {}

	public TimestampFilter(SortedKeyValueIterator<Key, Value> iterator, long start, boolean startInclusive, long end, boolean endInclusive) {
		super(iterator);
		this.start = start;
		this.startInclusive = startInclusive;
		this.end = end;
		this.endInclusive = endInclusive;
		findTop();
	}

	@Override
	public boolean accept(Key k, Value v) {
		long ts = k.getTimestamp();
		if (ts < start || ts > end) return false;
		if (!startInclusive && ts==start) return false;
		if (!endInclusive && ts==end) return false;
		return true;
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);

		if (options == null)
			throw new IllegalArgumentException("ttl must be set for AgeOffFilter");

		startInclusive = true;
		endInclusive = true;
		try {
			start = dateParser.parse(options.get(START)).getTime();
			end = dateParser.parse(options.get(END)).getTime();
		} catch (Exception e) {
			throw new IOException(e);
		}
		if (options.get(START_INCL)!=null)
			startInclusive = Boolean.parseBoolean(options.get(START_INCL));
		if (options.get(END_INCL)!=null)
			endInclusive = Boolean.parseBoolean(options.get(END_INCL));
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new TimestampFilter(getSource(),start,startInclusive,end,endInclusive);
	}

	@Override
	public IteratorOptions describeOptions() {
		IteratorOptions io = super.describeOptions();
		io.setName("tsfilter");
		io.setDescription("TimestampFilter displays entries with timestamps between specified values");
		io.addNamedOption("start","start timestamp (yyyyMMddHHmmssz)");
		io.addNamedOption("end","end timestamp (yyyyMMddHHmmssz)");
		io.addNamedOption("startInclusive", "true or false");
		io.addNamedOption("endInclusive", "true or false");
		return io;
	}

	@Override
	public boolean validateOptions(Map<String, String> options) {
		try {
			dateParser.parse(options.get(START));
			dateParser.parse(options.get(END));
			if (options.get(START_INCL)!=null)
				Boolean.parseBoolean(options.get(START_INCL));
			if (options.get(END_INCL)!=null)
				Boolean.parseBoolean(options.get(END_INCL));
		} catch (Exception e) {
			return false;
		}
		return true;
	}
}
