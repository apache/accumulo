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

package org.apache.accumulo.core.sample;

import java.util.Set;

import org.apache.accumulo.core.client.admin.SamplerConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;

/**
 * This sampler can hash any subset of a Key's fields.
 *
 * @since 1.8.0
 */

public class RowColumnSampler extends AbstractHashSampler {

  private boolean row = true;
  private boolean family = true;
  private boolean qualifier = true;
  private boolean visibility = true;

  private static final Set<String> VALID_OPTIONS = ImmutableSet.of("row", "family", "qualifier", "visibility");

  private boolean hashField(SamplerConfiguration config, String field) {
    String optValue = config.getOptions().get(field);
    if (optValue != null) {
      return Boolean.parseBoolean(optValue);
    }

    return false;
  }

  @Override
  protected boolean isValidOption(String option) {
    return super.isValidOption(option) || VALID_OPTIONS.contains(option);
  }

  @Override
  public void init(SamplerConfiguration config) {
    super.init(config);

    row = hashField(config, "row");
    family = hashField(config, "family");
    qualifier = hashField(config, "qualifier");
    visibility = hashField(config, "visibility");

    if (!row && !family && !qualifier && !visibility) {
      throw new IllegalStateException("Must hash at least one key field");
    }
  }

  private void putByteSquence(ByteSequence data, Hasher hasher) {
    hasher.putBytes(data.getBackingArray(), data.offset(), data.length());
  }

  @Override
  protected HashCode hash(HashFunction hashFunction, Key k) {
    Hasher hasher = hashFunction.newHasher();

    if (row) {
      putByteSquence(k.getRowData(), hasher);
    }

    if (family) {
      putByteSquence(k.getColumnFamilyData(), hasher);
    }

    if (qualifier) {
      putByteSquence(k.getColumnQualifierData(), hasher);
    }

    if (visibility) {
      putByteSquence(k.getColumnVisibilityData(), hasher);
    }

    return hasher.hash();
  }
}
