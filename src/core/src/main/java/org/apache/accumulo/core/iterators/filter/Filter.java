package org.apache.accumulo.core.iterators.filter;

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;


/**
 * @deprecated since 1.4
 * @use accumulo.core.iterators.Filter
 **/
public interface Filter {
	public void init(Map<String, String> options);
	public boolean accept(Key k, Value v);
}
