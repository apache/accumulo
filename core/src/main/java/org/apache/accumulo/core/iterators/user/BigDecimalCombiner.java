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
package org.apache.accumulo.core.iterators.user;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.ValueFormatException;

/**
 * A family of combiners that treat values as BigDecimals, encoding and decoding using the built-in BigDecimal String input/output functions.
 */
public abstract class BigDecimalCombiner extends TypedValueCombiner<BigDecimal> {
  private final static BigDecimalEncoder BDE = new BigDecimalEncoder();

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    setEncoder(BDE);
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("bigdecimalcombiner");
    io.setDescription("bigdecimalcombiner interprets Values as BigDecimals before combining");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (super.validateOptions(options) == false)
      return false;
    return true;
  }

  public static class BigDecimalSummingCombiner extends BigDecimalCombiner {
    @Override
    public BigDecimal typedReduce(Key key, Iterator<BigDecimal> iter) {
      if (!iter.hasNext())
        return null;
      BigDecimal sum = iter.next();
      while (iter.hasNext()) {
        sum = sum.add(iter.next());
      }
      return sum;
    }
  }

  public static class BigDecimalMaxCombiner extends BigDecimalCombiner {
    @Override
    public BigDecimal typedReduce(Key key, Iterator<BigDecimal> iter) {
      if (!iter.hasNext())
        return null;
      BigDecimal max = iter.next();
      while (iter.hasNext()) {
        max = max.max(iter.next());
      }
      return max;
    }
  }

  public static class BigDecimalMinCombiner extends BigDecimalCombiner {
    @Override
    public BigDecimal typedReduce(Key key, Iterator<BigDecimal> iter) {
      if (!iter.hasNext())
        return null;
      BigDecimal min = iter.next();
      while (iter.hasNext()) {
        min = min.min(iter.next());
      }
      return min;
    }
  }

  /**
   * Provides the ability to encode scientific notation.
   *
   */
  public static class BigDecimalEncoder implements org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder<BigDecimal> {
    @Override
    public byte[] encode(BigDecimal v) {
      return v.toString().getBytes(UTF_8);
    }

    @Override
    public BigDecimal decode(byte[] b) throws ValueFormatException {
      try {
        return new BigDecimal(new String(b, UTF_8));
      } catch (NumberFormatException nfe) {
        throw new ValueFormatException(nfe);
      }
    }
  }
}
