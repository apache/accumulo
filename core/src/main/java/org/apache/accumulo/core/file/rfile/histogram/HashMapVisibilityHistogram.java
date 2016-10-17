/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.file.rfile.histogram;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * A basic unbounded hash map implementation.
 */
public class HashMapVisibilityHistogram implements VisibilityHistogram {

  private HashMap<Text,AtomicLong> histogram;
  private ThreadLocal<Text> buffer = new ThreadLocal<Text>() {
    @Override public Text initialValue() {
      return new Text();
    }
  };

  public HashMapVisibilityHistogram() {
    this.histogram = new HashMap<>();
  }

  @Override
  public long increment(Key k) {
    final Text t = buffer.get();
    Objects.requireNonNull(k.getColumnVisibility(t));
    AtomicLong count = histogram.get(t);
    if (null == count) {
      count = new AtomicLong(0);
      // Make a copy of the buffer since we want to reuse `_text`
      Text copy = new Text(t);
      histogram.put(copy, count);
    }
    return count.incrementAndGet();
  }

  @Override
  public long get(Key k) {
    final Text t = buffer.get();
    Objects.requireNonNull(k.getColumnVisibility(t));
    AtomicLong counter = histogram.get(t);
    if (null == counter) {
      return 0l;
    }
    return counter.get();
  }

  @Override
  public void serialize(DataOutput writer) throws IOException {
    Objects.requireNonNull(writer).writeInt(histogram.size());
    for (Entry<Text,AtomicLong> entry : histogram.entrySet()) {
      writePair(writer, entry.getKey(), entry.getValue());
    }
  }

  /**
   * Write one visibility histogram entry.
   * @param writer
   * @param cv
   * @param count
   * @throws IOException
   */
  void writePair(DataOutput writer, Text cv, AtomicLong count) throws IOException {
    final int cvLength = cv.getLength();
    writer.writeInt(cvLength);
    writer.write(cv.getBytes(), 0, cvLength);
    writer.writeLong(count.get());
  }

  @Override
  public void deserialize(DataInput reader) throws IOException {
    final int numEntries = Objects.requireNonNull(reader).readInt();
    histogram = new HashMap<>();
    for (int i = 0; i < numEntries; i++) {
      histogram.put(readCV(reader), readCount(reader));
    }
  }

  /**
   * Read a serialized visibility.
   * @param reader
   * @return
   * @throws IOException
   */
  Text readCV(DataInput reader) throws IOException {
    byte[] cv = new byte[reader.readInt()];
    reader.readFully(cv);
    return new Text(cv);
  }

  /**
   * Read a serialized count.
   * @param reader
   * @return
   * @throws IOException
   */
  AtomicLong readCount(DataInput reader) throws IOException {
    return new AtomicLong(reader.readLong());
  }

  @Override
  public Iterator<Entry<Text,Long>> iterator() {
    return Iterators.transform(histogram.entrySet().iterator(), new Function<Entry<Text,AtomicLong>, Entry<Text,Long>>() {
      @Override public Entry<Text,Long> apply(Entry<Text,AtomicLong> input) {
        return new AbstractMap.SimpleEntry<Text,Long>(input.getKey(), Long.valueOf(input.getValue().longValue()));
      }
    });
  }

  HashMap<Text,AtomicLong> getData() {
    return histogram;
  }
}
