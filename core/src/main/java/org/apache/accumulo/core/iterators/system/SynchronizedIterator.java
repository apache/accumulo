package org.apache.accumulo.core.iterators.system;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/***
 * SynchronizedIterator: wrap a SortedKeyValueIterator so that all of its methods are synchronized
 */
public class SynchronizedIterator <K extends WritableComparable<?>,V extends Writable> implements SortedKeyValueIterator<K,V> {

  private SortedKeyValueIterator<K,V> source = null;
  
  @Override
  public synchronized void init(SortedKeyValueIterator<K,V> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
    source.init(source, options, env);
  }

  @Override
  public synchronized boolean hasTop() {
    return source.hasTop();
  }

  @Override
  public synchronized void next() throws IOException {
    source.next();
  }

  @Override
  public synchronized void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    source.seek(range, columnFamilies, inclusive);
  }

  @Override
  public synchronized K getTopKey() {
    return source.getTopKey();
  }

  @Override
  public synchronized V getTopValue() {
    return source.getTopValue();
  }

  @Override
  public synchronized SortedKeyValueIterator<K,V> deepCopy(IteratorEnvironment env) {
    return new SynchronizedIterator<K,V>(source.deepCopy(env));
  }
  
  public SynchronizedIterator(){}
  
  public SynchronizedIterator(SortedKeyValueIterator<K,V> source)
  {
    this.source = source;
  }
}
