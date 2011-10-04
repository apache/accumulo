package org.apache.accumulo.core.iterators;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;


/*
 * Subclasses should implement a switch on the "type" variable 
 * in their reduce method.
 */
public abstract class TypedValueCombiner<V> extends Combiner {
	protected Encoder<V> encoder;

	private static class VIterator<V> implements Iterator<V> {
		private Iterator<Value> source;
		private Encoder<V> encoder;

		VIterator(Iterator<Value> iter, Encoder<V> encoder) {
			this.source = iter;
			this.encoder = encoder;
		}

		@Override
		public boolean hasNext() {
			return source.hasNext();
		}

		@Override
		public V next() {
			if (!source.hasNext()) throw new NoSuchElementException();
			return encoder.decode(source.next().get());
		}

		@Override
		public void remove() {
			source.remove();
		}
	}

	public static interface Encoder<V> {
		public byte[] encode(V v);
		public V decode(byte[] b);
	}

	@Override
	public Value reduce(Key key, Iterator<Value> iter) {
		return new Value(encoder.encode(typedReduce(key, new VIterator<V>(iter,encoder))));
	}

	public abstract V typedReduce(Key key, Iterator<V> iter);
}
