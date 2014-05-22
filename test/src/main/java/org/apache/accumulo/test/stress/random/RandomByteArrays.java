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
package org.apache.accumulo.test.stress.random;

/**
 * An iterator that will create random byte arrays as it is looped over.
 */
public class RandomByteArrays extends Stream<byte[]> {
  private final RandomByteArrayMakerFunction rbamf;
  private final int min_array_size, max_array_size;
  
  public RandomByteArrays(RandomByteArrayMakerFunction rbamf, int min_array_size,
      int max_array_size) {
    this.rbamf = rbamf;
    this.min_array_size = min_array_size;
    this.max_array_size = max_array_size;
  }
  
  public byte[] next() {
    return rbamf.makeWithRandomSize(min_array_size, max_array_size);
  }
}
