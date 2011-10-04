package org.apache.accumulo.core.iterators.filter;

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;


public interface Filter {
	public void init(Map<String, String> options);
	public boolean accept(Key k, Value v);
}
