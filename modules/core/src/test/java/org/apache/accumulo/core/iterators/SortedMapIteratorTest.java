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

import java.util.TreeMap;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.impl.BaseIteratorEnvironment;
import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

public class SortedMapIteratorTest {

  @Test(expected = SampleNotPresentException.class)
  public void testSampleNotPresent() {
    SortedMapIterator smi = new SortedMapIterator(new TreeMap<Key,Value>());
    smi.deepCopy(new BaseIteratorEnvironment() {
      @Override
      public boolean isSamplingEnabled() {
        return true;
      }

      @Override
      public SamplerConfiguration getSamplerConfiguration() {
        return new SamplerConfiguration(RowSampler.class.getName());
      }
    });
  }
}
