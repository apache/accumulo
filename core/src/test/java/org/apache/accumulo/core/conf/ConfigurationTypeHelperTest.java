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
package org.apache.accumulo.core.conf;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

public class ConfigurationTypeHelperTest {

  @Test
  public void testGetMemoryInBytes() {
    Stream.<Function<String,Long>>of(ConfigurationTypeHelper::getFixedMemoryAsBytes,
        ConfigurationTypeHelper::getMemoryAsBytes).forEach(memFunc -> {
          assertEquals(42L, memFunc.apply("42").longValue());
          assertEquals(42L, memFunc.apply("42b").longValue());
          assertEquals(42L, memFunc.apply("42B").longValue());
          assertEquals(42L * 1024L, memFunc.apply("42K").longValue());
          assertEquals(42L * 1024L, memFunc.apply("42k").longValue());
          assertEquals(42L * 1024L * 1024L, memFunc.apply("42M").longValue());
          assertEquals(42L * 1024L * 1024L, memFunc.apply("42m").longValue());
          assertEquals(42L * 1024L * 1024L * 1024L, memFunc.apply("42G").longValue());
          assertEquals(42L * 1024L * 1024L * 1024L, memFunc.apply("42g").longValue());
        });
    assertEquals(Runtime.getRuntime().maxMemory() / 10,
        ConfigurationTypeHelper.getMemoryAsBytes("10%"));
    assertEquals(Runtime.getRuntime().maxMemory() / 5,
        ConfigurationTypeHelper.getMemoryAsBytes("20%"));
  }

  @Test
  public void testGetFixedMemoryAsBytesFailureCases1() {
    assertThrows(IllegalArgumentException.class,
        () -> ConfigurationTypeHelper.getFixedMemoryAsBytes("42x"));
  }

  @Test
  public void testGetFixedMemoryAsBytesFailureCases2() {
    assertThrows(IllegalArgumentException.class,
        () -> ConfigurationTypeHelper.getFixedMemoryAsBytes("FooBar"));
  }

  @Test
  public void testGetFixedMemoryAsBytesFailureCases3() {
    assertThrows(IllegalArgumentException.class,
        () -> ConfigurationTypeHelper.getFixedMemoryAsBytes("40%"));
  }

  @Test
  public void testGetMemoryAsBytesFailureCases1() {
    assertThrows(IllegalArgumentException.class,
        () -> ConfigurationTypeHelper.getMemoryAsBytes("42x"));
  }

  @Test
  public void testGetMemoryAsBytesFailureCases2() {
    assertThrows(IllegalArgumentException.class,
        () -> ConfigurationTypeHelper.getMemoryAsBytes("FooBar"));
  }

  @Test
  public void testGetTimeInMillis() {
    assertEquals(DAYS.toMillis(42), ConfigurationTypeHelper.getTimeInMillis("42d"));
    assertEquals(HOURS.toMillis(42), ConfigurationTypeHelper.getTimeInMillis("42h"));
    assertEquals(MINUTES.toMillis(42), ConfigurationTypeHelper.getTimeInMillis("42m"));
    assertEquals(SECONDS.toMillis(42), ConfigurationTypeHelper.getTimeInMillis("42s"));
    assertEquals(SECONDS.toMillis(42), ConfigurationTypeHelper.getTimeInMillis("42"));
    assertEquals(42L, ConfigurationTypeHelper.getTimeInMillis("42ms"));
  }

  @Test
  public void testGetTimeInMillisFailureCase1() {
    assertThrows(IllegalArgumentException.class,
        () -> ConfigurationTypeHelper.getTimeInMillis("abc"));
  }

  @Test
  public void testGetTimeInMillisFailureCase2() {
    assertThrows(IllegalArgumentException.class,
        () -> ConfigurationTypeHelper.getTimeInMillis("ms"));
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

  @Test
  public void testGetFractionFailureCase1() {
    assertThrows(IllegalArgumentException.class, () -> ConfigurationTypeHelper.getFraction("%"));
  }

  @Test
  public void testGetFractionFailureCase2() {
    assertThrows(IllegalArgumentException.class,
        () -> ConfigurationTypeHelper.getFraction("abc0%"));
  }

  @Test
  public void testGetFractionFailureCase3() {
    assertThrows(IllegalArgumentException.class, () -> ConfigurationTypeHelper.getFraction(".%"));
  }
}
