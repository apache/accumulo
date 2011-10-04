package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.filter.ColumnQualifierFilter;
import org.apache.accumulo.core.iterators.filter.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;


public class SystemScanIterator extends FilteringIterator {
	public SystemScanIterator(
			SortedKeyValueIterator<Key, Value> iterator,
			Authorizations authorizations, byte[] defaultLabels, HashSet<Column> hsc) throws IOException {
		super(iterator, Arrays.asList(new ColumnQualifierFilter(hsc), new VisibilityFilter(authorizations, defaultLabels)));
	}

}
