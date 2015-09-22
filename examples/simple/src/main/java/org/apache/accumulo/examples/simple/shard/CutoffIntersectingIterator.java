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

package org.apache.accumulo.examples.simple.shard;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.SamplerConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.accumulo.core.sample.RowColumnSampler;

import com.google.common.base.Preconditions;

/**
 * This iterator uses a sample built from the Column Qualifier to quickly avoid intersecting iterator queries that may return too many documents.
 */

public class CutoffIntersectingIterator extends IntersectingIterator {

  private IntersectingIterator sampleII;
  private int sampleMax;
  private boolean hasTop;

  public static void setCutoff(IteratorSetting iterCfg, int cutoff) {
    Preconditions.checkArgument(cutoff >= 0);
    iterCfg.addOption("cutoff", cutoff + "");
  }

  @Override
  public boolean hasTop() {
    return hasTop && super.hasTop();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> seekColumnFamilies, boolean inclusive) throws IOException {

    sampleII.seek(range, seekColumnFamilies, inclusive);

    // this check will be redone whenever iterator stack is torn down and recreated.
    int count = 0;
    while (count <= sampleMax && sampleII.hasTop()) {
      sampleII.next();
      count++;
    }

    if (count > sampleMax) {
      // In a real application would probably want to return a key value that indicates too much data. Since this would execute for each tablet, some tablets
      // may return data. For tablets that did not return data, would want an indication.
      hasTop = false;
    } else {
      hasTop = true;
      super.seek(range, seekColumnFamilies, inclusive);
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);

    IteratorEnvironment sampleEnv = env.cloneWithSamplingEnabled();

    setMax(sampleEnv, options);

    SortedKeyValueIterator<Key,Value> sampleDC = source.deepCopy(sampleEnv);
    sampleII = new IntersectingIterator();
    sampleII.init(sampleDC, options, env);

  }

  static void validateSamplerConfig(SamplerConfiguration sampleConfig) {
    Preconditions.checkNotNull(sampleConfig);
    Preconditions.checkArgument(sampleConfig.getSamplerClassName().equals(RowColumnSampler.class.getName()),
        "Unexpected Sampler " + sampleConfig.getSamplerClassName());
    Preconditions.checkArgument(sampleConfig.getOptions().get("qualifier").equals("true"), "Expected sample on column qualifier");
    Preconditions.checkArgument(isNullOrFalse(sampleConfig.getOptions(), "row", "family", "visibility"), "Expected sample on column qualifier only");
  }

  private void setMax(IteratorEnvironment sampleEnv, Map<String,String> options) {
    String cutoffValue = options.get("cutoff");
    SamplerConfiguration sampleConfig = sampleEnv.getSamplerConfiguration();

    // Ensure the sample was constructed in an expected way. If the sample is not built as expected, then can not draw conclusions based on sample.
    Preconditions.checkNotNull(cutoffValue, "Expected cutoff option is missing");
    validateSamplerConfig(sampleConfig);

    int modulus = Integer.parseInt(sampleConfig.getOptions().get("modulus"));

    sampleMax = Math.round(Float.parseFloat(cutoffValue) / modulus);
  }

  private static boolean isNullOrFalse(Map<String,String> options, String... keys) {
    for (String key : keys) {
      String val = options.get(key);
      if (val != null && val.equals("true")) {
        return false;
      }
    }
    return true;
  }
}
