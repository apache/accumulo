package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;


public abstract class SkippingIterator extends WrappingIterator {

	@Override
	public void next() throws IOException {
		getSource().next();
		consume();
	}

	protected abstract void consume() throws IOException;

	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		super.seek(range, columnFamilies, inclusive);
		consume();
	}

}
