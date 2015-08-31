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

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;

/**
 * Builds a sample based on entire rows. If a row is selected for the sample, then all of its columns will be included.
 *
 * @since 1.8.0
 */

public class RowSampler extends AbstractHashSampler {

  @Override
  protected HashCode hash(HashFunction hashFunction, Key k) {
    ByteSequence row = k.getRowData();
    return hashFunction.hashBytes(row.getBackingArray(), row.offset(), row.length());
  }
}
