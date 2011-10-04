package org.apache.accumulo.core.iterators;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;


public interface InterruptibleIterator extends SortedKeyValueIterator<Key, Value>{
	public void setInterruptFlag(AtomicBoolean flag);
}
