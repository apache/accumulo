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

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFile.LocalityGroupWriter;
import org.apache.hadoop.io.Text;

/**
 * 
 */
public class VisibilityHistogramLocalityGroupWriter {
  private final HashMap<Text,AtomicLong> histogram;
  private final LocalityGroupWriter lgr;

  private ThreadLocal<Text> buffer = new ThreadLocal<Text>() {
    @Override public Text initialValue() {
      return new Text();
    }
  };

  public VisibilityHistogramLocalityGroupWriter(LocalityGroupWriter lgr) {
    this.lgr = lgr;
    this.histogram = new HashMap<>(); 
  }

  public void append(Key key, Value value) throws IOException {
    Text _text = buffer.get();
    key.getColumnVisibility(_text);
    AtomicLong count = histogram.get(_text);
    if (null == count) {
      count = new AtomicLong(0);
      // Make a copy of the buffer since we want to reuse `_text`
      Text copy = new Text(_text);
      histogram.put(copy, count);
    }
    count.incrementAndGet();
  }

  public void close() throws IOException {
    
  }
}
