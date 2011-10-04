package org.apache.accumulo.core.iterators.filter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Caller sets up this filter for the rows that they want to match on and then this
 * filter ignores those rows during scans and compactions.
 * 
 */
public class DeleteFilter extends RegExFilter {

	@Override
	public boolean accept(Key key, Value value) {
		return !(super.accept(key, value));
	}

}
