package org.apache.accumulo.core.util;

import java.util.Iterator;

public class PeekingIterator<E> implements Iterator<E> {
	Iterator<E> source;
	E top;

	public PeekingIterator(Iterator<E> source) {
		this.source = source;
		if (source.hasNext()) top = source.next();
		else top = null;
	}

	public E peek() {
		return top;
	}

	public E next() {
		E lastPeeked = top;
		if (source.hasNext()) top = source.next();
		else top = null;
		return lastPeeked;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasNext() {
		return top!=null;
	}
}
