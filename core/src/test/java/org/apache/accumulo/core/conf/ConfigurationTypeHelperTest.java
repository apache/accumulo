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
package org.apache.accumulo.core.conf;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.function.Function;

import org.junit.Test;

public class ConfigurationTypeHelperTest {

  @Test
  public void testGetMemoryInBytes() throws Exception {
    Arrays.<Function<String,Long>> asList(ConfigurationTypeHelper::getFixedMemoryAsBytes, ConfigurationTypeHelper::getMemoryAsBytes).stream()
        .forEach(memFunc -> {
          assertEquals(42l, memFunc.apply("42").longValue());
          assertEquals(42l, memFunc.apply("42b").longValue());
          assertEquals(42l, memFunc.apply("42B").longValue());
          assertEquals(42l * 1024l, memFunc.apply("42K").longValue());
          assertEquals(42l * 1024l, memFunc.apply("42k").longValue());
          assertEquals(42l * 1024l * 1024l, memFunc.apply("42M").longValue());
          assertEquals(42l * 1024l * 1024l, memFunc.apply("42m").longValue());
          assertEquals(42l * 1024l * 1024l * 1024l, memFunc.apply("42G").longValue());
          assertEquals(42l * 1024l * 1024l * 1024l, memFunc.apply("42g").longValue());
        });
    assertEquals(Runtime.getRuntime().maxMemory() / 10, ConfigurationTypeHelper.getMemoryAsBytes("10%"));
    assertEquals(Runtime.getRuntime().maxMemory() / 5, ConfigurationTypeHelper.getMemoryAsBytes("20%"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetFixedMemoryAsBytesFailureCases1() throws Exception {
    ConfigurationTypeHelper.getFixedMemoryAsBytes("42x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetFixedMemoryAsBytesFailureCases2() throws Exception {
    ConfigurationTypeHelper.getFixedMemoryAsBytes("FooBar");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetFixedMemoryAsBytesFailureCases3() throws Exception {
    ConfigurationTypeHelper.getFixedMemoryAsBytes("40%");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetMemoryAsBytesFailureCases1() throws Exception {
    ConfigurationTypeHelper.getMemoryAsBytes("42x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetMemoryAsBytesFailureCases2() throws Exception {
    ConfigurationTypeHelper.getMemoryAsBytes("FooBar");
  }

  @Test
  public void testGetTimeInMillis() {
    assertEquals(42L * 24 * 60 * 60 * 1000, ConfigurationTypeHelper.getTimeInMillis("42d"));
    assertEquals(42L * 60 * 60 * 1000, ConfigurationTypeHelper.getTimeInMillis("42h"));
    assertEquals(42L * 60 * 1000, ConfigurationTypeHelper.getTimeInMillis("42m"));
    assertEquals(42L * 1000, ConfigurationTypeHelper.getTimeInMillis("42s"));
    assertEquals(42L * 1000, ConfigurationTypeHelper.getTimeInMillis("42"));
    assertEquals(42L, ConfigurationTypeHelper.getTimeInMillis("42ms"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetTimeInMillisFailureCase1() {
    ConfigurationTypeHelper.getTimeInMillis("abc");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetTimeInMillisFailureCase2() {
    ConfigurationTypeHelper.getTimeInMillis("ms");
  }

  @Test
  public void testGetFraction() {
    double delta = 0.0000000000001;
    assertEquals(0.5d, ConfigurationTypeHelper.getFraction("0.5"), delta);
    assertEquals(3.0d, ConfigurationTypeHelper.getFraction("3"), delta);
    assertEquals(-0.25d, ConfigurationTypeHelper.getFraction("-25%"), delta);
    assertEquals(0.99546d, ConfigurationTypeHelper.getFraction("99.546%"), delta);
    assertEquals(0.0d, ConfigurationTypeHelper.getFraction("0%"), delta);
    assertEquals(0.0d, ConfigurationTypeHelper.getFraction("-0.000"), delta);
    assertEquals(0.001d, ConfigurationTypeHelper.getFraction(".1%"), delta);
    assertEquals(1d, ConfigurationTypeHelper.getFraction("1."), delta);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetFractionFailureCase1() {
    ConfigurationTypeHelper.getFraction("%");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetFractionFailureCase2() {
    ConfigurationTypeHelper.getFraction("abc0%");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetFractionFailureCase3() {
    ConfigurationTypeHelper.getFraction(".%");
  }
}
