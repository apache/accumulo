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
package org.apache.accumulo.core.util;

import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Min;
import org.apache.commons.math3.stat.descriptive.summary.Sum;

public class Stat {
  Min min;
  Max max;
  Sum sum;
  Mean mean;
  StandardDeviation sd;

  StorelessUnivariateStatistic[] stats;

  public Stat() {
    min = new Min();
    max = new Max();
    sum = new Sum();
    mean = new Mean();
    sd = new StandardDeviation();

    stats = new StorelessUnivariateStatistic[] {min, max, sum, mean, sd};
  }

  public void addStat(long stat) {
    for (StorelessUnivariateStatistic statistic : stats) {
      statistic.increment(stat);
    }
  }

  public long getMin() {
    return (long) min.getResult();
  }

  public long getMax() {
    return (long) max.getResult();
  }

  public long getSum() {
    return (long) sum.getResult();
  }

  public double getAverage() {
    return mean.getResult();
  }

  public double getStdDev() {
    return sd.getResult();
  }

  @Override
  public String toString() {
    return String.format("%,d %,d %,.2f %,d", getMin(), getMax(), getAverage(), mean.getN());
  }

  public void clear() {
    for (StorelessUnivariateStatistic statistic : stats) {
      statistic.clear();
    }
  }

}
