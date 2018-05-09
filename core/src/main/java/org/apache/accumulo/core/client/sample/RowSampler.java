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

package org.apache.accumulo.core.client.sample;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

/**
 * Builds a sample based on entire rows. If a row is selected for the sample, then all of its
 * columns will be included.
 *
 * <p>
 * To determine what options are valid for hashing see {@link AbstractHashSampler}. This class
 * offers no addition options, it always hashes on the row.
 *
 * <p>
 * To configure Accumulo to generate sample data on one thousandth of the rows, the following
 * SamplerConfiguration could be created and used to configure a table.
 *
 * <pre>
 * <code>
 * new SamplerConfiguration(RowSampler.class.getName()).setOptions(
 *   ImmutableMap.of("hasher","murmur3_32","modulus","1009"));
 * </code>
 * </pre>
 *
 * @since 1.8.0
 */
public class RowSampler extends AbstractHashSampler {

  @Override
  protected void hash(DataOutput hasher, Key k) throws IOException {
    ByteSequence row = k.getRowData();
    hasher.write(row.getBackingArray(), row.offset(), row.length());
  }
}
