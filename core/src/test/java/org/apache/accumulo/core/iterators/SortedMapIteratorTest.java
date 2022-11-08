/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.iterators;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.TreeMap;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

public class SortedMapIteratorTest {

  @Test
  public void testSampleNotPresent() {
    SortedMapIterator smi = new SortedMapIterator(new TreeMap<>());
    assertThrows(SampleNotPresentException.class, () -> smi.deepCopy(new IteratorEnvironment() {
      @Override
      public boolean isSamplingEnabled() {
        return true;
      }

      @Override
      public SamplerConfiguration getSamplerConfiguration() {
        return new SamplerConfiguration(RowSampler.class.getName());
      }
    }));
  }
}
