package org.apache.accumulo.core.iterators.system;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;


public interface InterruptibleIterator extends SortedKeyValueIterator<Key, Value>{
	public void setInterruptFlag(AtomicBoolean flag);
}
