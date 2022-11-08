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
package org.apache.accumulo.core.file.rfile;

import org.apache.commons.math3.stat.StatUtils;
import org.apache.commons.math3.util.FastMath;

/**
 * This class supports efficient window statistics. Apache commons math3 has a class called
 * DescriptiveStatistics that supports windows. DescriptiveStatistics recomputes the statistics over
 * the entire window each time its requested. In a test over 1,000,000 entries with a window size of
 * 1019 that requested stats for each entry this class took ~50ms and DescriptiveStatistics took
 * ~6,000ms.
 *
 * <p>
 * This class may not be as accurate as DescriptiveStatistics. In unit test its within 1/1000 of
 * DescriptiveStatistics.
 */
class RollingStats {
  private int position;
  private double[] window;

  private double average;
  private double variance;
  private double stddev;

  // indicates if the window is full
  private boolean windowFull;

  private int recomputeCounter = 0;

  RollingStats(int windowSize) {
    this.windowFull = false;
    this.position = 0;
    this.window = new double[windowSize];
  }

  /**
   * @see <a href=
   *      "https://jonisalonen.com/2014/efficient-and-accurate-rolling-standard-deviation">Efficient
   *      and accurate rolling standard deviation</a>
   */
  private void update(double newValue, double oldValue, int windowSize) {
    double delta = newValue - oldValue;

    double oldAverage = average;
    average = average + delta / windowSize;
    variance += delta * (newValue - average + oldValue - oldAverage) / (windowSize - 1);
    stddev = FastMath.sqrt(variance);
  }

  void addValue(long stat) {

    double old = window[position];
    window[position] = stat;
    position++;
    recomputeCounter++;

    if (windowFull) {
      update(stat, old, window.length);
    } else if (position == window.length) {
      computeStats(window.length);
      windowFull = true;
    }

    if (position == window.length) {
      position = 0;
    }
  }

  private void computeStats(int len) {
    average = StatUtils.mean(window, 0, len);
    variance = StatUtils.variance(window, average, 0, len);
    stddev = FastMath.sqrt(variance);
    recomputeCounter = 0;
  }

  private void computeStats() {
    if (windowFull) {
      if (variance < 0 || recomputeCounter >= 100) {
        // incremental computation drifts over time, so periodically force a recompute
        computeStats(window.length);
      }
    } else if (recomputeCounter > 0) {
      computeStats(position);
    }
  }

  double getMean() {
    computeStats();
    return average;
  }

  double getVariance() {
    computeStats();
    return variance;
  }

  double getStandardDeviation() {
    computeStats();
    return stddev;
  }

  boolean isWindowFull() {
    return windowFull;
  }
}
