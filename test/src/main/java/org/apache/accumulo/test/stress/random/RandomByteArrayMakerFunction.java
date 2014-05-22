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

import java.util.Random;

import com.google.common.base.Preconditions;

/**
 * This class is one RBAMF. Don't mess with it, or it'll mess with you.
 */
public class RandomByteArrayMakerFunction {
  private final Random random;
  
  public RandomByteArrayMakerFunction() {
    this(0);
  }
  
  public RandomByteArrayMakerFunction(int seed) {
    random = new Random(seed);
  }
  
  public byte[] make(int size) {
    byte[] b = new byte[size];
    random.nextBytes(b);
    return b;
  }
  
  public byte[] makeWithRandomSize(int max) {
    return makeWithRandomSize(1, random.nextInt(max));
  }
  
  public byte[] makeWithRandomSize(int min, int max) {
    Preconditions.checkArgument(min > 0, "Min must be positive.");
    Preconditions.checkArgument(max >= min, "Max must be greater than or equal to min.");
    if (min == max) {
      return make(min);
    } else {
      final int spread = max - min;
      final int random_value = random.nextInt(spread);
      // we pick a random number that's between 0 and (max - min), then add
      // min as an offset to get a random number that's [min, max)
      return make(random_value + min);
    }
  }
}
