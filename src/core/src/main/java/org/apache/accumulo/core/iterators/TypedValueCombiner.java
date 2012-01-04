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
import org.apache.accumulo.start.classloader.AccumuloClassLoader;

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
  private Encoder<V> encoder = null;
  
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
  
  /**
   * Sets the Encoder<V> used to translate Values to V and back.
   * 
   * @param encoder
   */
  protected void setEncoder(Encoder<V> encoder) {
    this.encoder = encoder;
  }
  
  /**
   * Instantiates and sets the Encoder<V> used to translate Values to V and back.
   * 
   * @param encoderClass
   * @throws IllegalArgumentException
   *           if ClassNotFoundException, InstantiationException, or IllegalAccessException occurs
   */
  protected void setEncoder(String encoderClass) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends Encoder<V>> clazz = (Class<? extends Encoder<V>>) AccumuloClassLoader.loadClass(encoderClass, Encoder.class);
      encoder = clazz.newInstance();
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    } catch (InstantiationException e) {
      throw new IllegalArgumentException(e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException(e);
    }
  }
  
  /**
   * Tests whether v remains the same when encoded and decoded with the current encoder.
   * 
   * @param v
   * @throws IllegalStateException
   *           if an encoder has not been set.
   * @throws IllegalArgumentException
   *           if the test fails.
   */
  protected void testEncoder(V v) {
    if (encoder == null)
      throw new IllegalStateException("encoder has not been initialized");
    testEncoder(encoder, v);
  }
  
  /**
   * Tests whether v remains the same when encoded and decoded with the given encoder.
   * 
   * @param encoder
   * @param v
   * @throws IllegalArgumentException
   *           if the test fails.
   */
  public static <V> void testEncoder(Encoder<V> encoder, V v) {
    try {
      if (!v.equals(encoder.decode(encoder.encode(v))))
        throw new IllegalArgumentException("something wrong with " + encoder.getClass().getName() + " -- doesn't encode and decode " + v + " properly");
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(encoder.getClass().getName() + " doesn't encode " + v.getClass().getName());
    }
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
