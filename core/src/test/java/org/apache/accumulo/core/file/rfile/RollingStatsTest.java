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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.util.function.IntSupplier;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.jupiter.api.Test;

import com.google.common.math.DoubleMath;

public class RollingStatsTest {

  private static final double TOLERANCE = 1.0 / 1000;
  private static final SecureRandom random = new SecureRandom();

  private static void assertFuzzyEquals(double expected, double actual) {
    assertTrue(DoubleMath.fuzzyEquals(expected, actual, TOLERANCE), String.format(
        "expected: %f, actual: %f diff: %f", expected, actual, Math.abs(expected - actual)));
  }

  private static void checkAgreement(DescriptiveStatistics ds, RollingStats rs) {
    // getting stats from ds is expensive, so do it once... otherwise unit test takes 11 sec
    // instead of 5 secs
    double expMean = ds.getMean();
    double expVar = ds.getVariance();
    double expStdDev = Math.sqrt(expVar);

    assertFuzzyEquals(expMean, rs.getMean());
    assertFuzzyEquals(expVar, rs.getVariance());
    assertFuzzyEquals(expStdDev, rs.getStandardDeviation());

    assertTrue(expMean >= 0);
    assertTrue(rs.getMean() >= 0);
    assertTrue(expVar >= 0);
    assertTrue(rs.getVariance() >= 0);
    assertTrue(expStdDev >= 0);
    assertTrue(rs.getStandardDeviation() >= 0);
  }

  private static class StatTester {

    private DescriptiveStatistics ds;
    private RollingStats rs;
    private RollingStats rsp;

    StatTester(int windowSize) {
      ds = new DescriptiveStatistics();
      ds.setWindowSize(windowSize);

      rs = new RollingStats(windowSize);
      rsp = new RollingStats(windowSize);
    }

    void addValue(long v) {
      ds.addValue(v);
      rs.addValue(v);
      rsp.addValue(v);
      checkAgreement(ds, rs);

      if (random.nextDouble() < 0.001) {
        checkAgreement(ds, rsp);
      }
    }

    void check() {
      checkAgreement(ds, rsp);
    }
  }

  @Test
  public void testFewSizes() {
    StatTester st = new StatTester(1019);
    int[] keySizes = {103, 113, 123, 2345};
    for (int i = 0; i < 10000; i++) {
      st.addValue(keySizes[random.nextInt(keySizes.length)]);
    }
    st.check();
  }

  @Test
  public void testConstant() {

    StatTester st = new StatTester(1019);

    for (int i = 0; i < 10000; i++) {
      st.addValue(111);
    }

    st.check();
  }

  @Test
  public void testUniformIncreasing() {

    for (int windowSize : new int[] {10, 13, 20, 100, 500}) {

      StatTester st = new StatTester(windowSize);

      for (int i = 0; i < 1000; i++) {
        int v = 200 + random.nextInt(50);

        st.addValue(v);
      }

      st.check();
    }
  }

  @Test
  public void testSlowIncreases() {
    // number of keys with the same len
    int len = 100;

    StatTester st = new StatTester(1019);

    for (int i = 0; i < 50; i++) {
      for (int j = 0; j < 3000; j++) {
        st.addValue(len);
      }

      len = (int) (len * 1.1);
    }

    st.check();
  }

  private void testDistribrution(IntSupplier d) {
    StatTester st = new StatTester(2017);

    for (int i = 0; i < 7000; i++) {
      st.addValue(d.getAsInt());
    }

    st.check();
  }

  @Test
  public void testZipf() {
    ZipfDistribution zd = new ZipfDistribution(new Well19937c(42), 1000, 2);
    testDistribrution(() -> zd.sample() * 100);
  }

  @Test
  public void testNormal() {
    NormalDistribution nd = new NormalDistribution(new Well19937c(42), 200, 20);
    testDistribrution(() -> (int) nd.sample());
  }

  @Test
  public void testSpikes() {

    StatTester st = new StatTester(3017);

    for (int i = 0; i < 13; i++) {

      // write small keys
      int numSmall = 1000 + random.nextInt(1000);
      for (int s = 0; s < numSmall; s++) {
        int sks = 50 + random.nextInt(100);
        // simulate row with multiple cols
        for (int c = 0; c < 3; c++) {
          st.addValue(sks);
        }
      }

      // write a few large keys
      int numLarge = 1 + random.nextInt(1);
      for (int l = 0; l < numLarge; l++) {
        int lks = 500000 + random.nextInt(1000000);
        for (int c = 0; c < 3; c++) {
          st.addValue(lks);
        }
      }
    }

    st.check();
  }
}
