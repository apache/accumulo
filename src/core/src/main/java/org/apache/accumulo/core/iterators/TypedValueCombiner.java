/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.iterators;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * A Combiner that decodes each Value to type V before reducing, then encodes the result of typedReduce back to Value.
 * 
 * Subclasses must implement a typedReduce method: public V typedReduce(Key key, Iterator<V> iter);
 * 
 * This typedReduce method will be passed the most recent Key and an iterator over the Values (translated to Vs) for all non-deleted versions of that Key.
 * 
 * Subclasses may implement a switch on the "type" variable to choose an Encoder in their init method.
 */
public abstract class TypedValueCombiner<V> extends Combiner {
  protected Encoder<V> encoder;
  
  /**
   * A Java Iterator that translates an Iterator<Value> to an Iterator<V> using the decode method of an Encoder.
   */
  private static class VIterator<V> implements Iterator<V> {
    private Iterator<Value> source;
    private Encoder<V> encoder;
    
    /**
     * Constructs an Iterator<V> from an Iterator<Value>
     * 
     * @param iter
     *          The source iterator
     * 
     * @param encoder
     *          The Encoder whose decode method is used to translate from Value to V
     */
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
      if (!source.hasNext())
        throw new NoSuchElementException();
      return encoder.decode(source.next().get());
    }
    
    @Override
    public void remove() {
      source.remove();
    }
  }
  
  /**
   * An interface for translating from byte[] to V and back.
   */
  public static interface Encoder<V> {
    public byte[] encode(V v);
    
    public V decode(byte[] b);
  }
  
  @Override
  public Value reduce(Key key, Iterator<Value> iter) {
    return new Value(encoder.encode(typedReduce(key, new VIterator<V>(iter, encoder))));
  }
  
  /**
   * Reduces a list of V into a single V.
   * 
   * @param key
   *          The most recent version of the Key being reduced.
   * 
   * @param iter
   *          An iterator over the V for different versions of the key.
   * 
   * @return The combined V.
   */
  public abstract V typedReduce(Key key, Iterator<V> iter);
}
