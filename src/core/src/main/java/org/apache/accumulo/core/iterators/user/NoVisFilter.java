package org.apache.accumulo.core.iterators.user;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;

public class NoVisFilter extends Filter implements OptionDescriber {

	public NoVisFilter() {}

	public NoVisFilter(SortedKeyValueIterator<Key, Value> iterator) {
		super(iterator);
	}

	@Override
	public boolean accept(Key k, Value v) {
		ColumnVisibility vis = new ColumnVisibility(k.getColumnVisibility());
		return vis.getExpression().length > 0;
	}

	@Override
	public IteratorOptions describeOptions() {
		IteratorOptions io = super.describeOptions();
		io.setName("novis");
		io.setDescription("NoLabelFilter hides entries without a visibility label");
		return io;
	}
}
