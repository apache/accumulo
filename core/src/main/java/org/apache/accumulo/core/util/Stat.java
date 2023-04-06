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
package org.apache.accumulo.core.util;

import org.apache.accumulo.core.spi.common.Stats;
import org.apache.commons.math3.stat.descriptive.moment.Mean;

public class Stat implements Stats {
  long min;
  long max;
  long sum;
  Mean mean;

  public Stat() {
    mean = new Mean();
    clear();
  }

  public void addStat(long stat) {
    min = Math.min(min, stat);
    max = Math.max(max, stat);
    sum += stat;
    mean.increment(stat);
  }

  @Override
  public long min() {
    return num() == 0 ? 0L : min;
  }

  @Override
  public long max() {
    return num() == 0 ? 0L : max;
  }

  @Override
  public long sum() {
    return sum;
  }

  @Override
  public double mean() {
    return mean.getResult();
  }

  @Override
  public String toString() {
    return String.format("%,d %,d %,.2f %,d", min(), max(), mean(), mean.getN());
  }

  public void clear() {
    min = Long.MAX_VALUE;
    max = Long.MIN_VALUE;
    sum = 0;
    mean.clear();
  }

  @Override
  public long num() {
    return mean.getN();
  }

  public Stat copy() {
    Stat stat = new Stat();
    stat.min = this.min;
    stat.max = this.max;
    stat.sum = this.sum;
    stat.mean = this.mean.copy();
    return stat;
  }
}
