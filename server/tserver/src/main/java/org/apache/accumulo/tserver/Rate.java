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
package org.apache.accumulo.tserver;

public class Rate {
  private long lastCounter = -1;
  private long lastTime = -1;
  private double current = 0.0;
  final double ratio;

  /**
   * Turn a counter into an exponentially smoothed rate over time.
   *
   * @param ratio
   *          the rate at which each update influences the curve; must be (0., 1.0)
   */
  public Rate(double ratio) {
    if (ratio <= 0. || ratio >= 1.0)
      throw new IllegalArgumentException("ratio must be > 0. and < 1.0");
    this.ratio = ratio;
  }

  public double update(long counter) {
    return update(System.currentTimeMillis(), counter);
  }

  synchronized public double update(long when, long counter) {
    if (lastCounter < 0) {
      lastTime = when;
      lastCounter = counter;
      return current;
    }
    if (lastTime == when) {
      throw new IllegalArgumentException("update time < last value");
    }
    double keep = 1. - ratio;
    current = (keep * current + ratio * ((counter - lastCounter)) * 1000. / (when - lastTime));
    lastTime = when;
    lastCounter = counter;
    return current;
  }

  synchronized public double rate() {
    return this.current;
  }
}
