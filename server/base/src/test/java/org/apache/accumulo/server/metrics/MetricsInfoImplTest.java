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
package org.apache.accumulo.server.metrics;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.regex.PatternSyntaxException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.server.ServerContext;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class MetricsInfoImplTest {
  @Test
  public void factoryTest() throws Exception {

    ServerContext context = mock(ServerContext.class);
    AccumuloConfiguration conf = mock(AccumuloConfiguration.class);
    expect(context.getConfiguration()).andReturn(conf).anyTimes();
    expect(conf.getAllPropertiesWithPrefixStripped(anyObject())).andReturn(Map.of()).anyTimes();
    expect(conf.newDeriver(anyObject())).andReturn(Map::of).anyTimes();
    replay(context, conf);
    assertNotNull(MetricsInfoImpl.getRegistryFromFactory(SPIFactory.class.getName(), context));

    assertThrows(ClassNotFoundException.class,
        () -> MetricsInfoImpl.getRegistryFromFactory(String.class.getName(), context));

    verify(context, conf);
  }

  static class SPIFactory implements org.apache.accumulo.core.spi.metrics.MeterRegistryFactory {

    SPIFactory() {

    }

    @Override
    public MeterRegistry create(final InitParameters params) {
      return new SimpleMeterRegistry();
    }
  }

  /**
   * Verify that when a Null pattern is supplied to {@link MetricsInfoImpl#getMeterFilter(String)},
   * that we catch a NullPointerException and receive the errorMessage: "patternList must not be
   * null"
   */
  @Test
  public void testNullPatternList() {
    Throwable exception = assertThrows(NullPointerException.class,
        () -> MetricsInfoImpl.getMeterFilter(null), "Expected an NPE");
    assertEquals("patternList must not be null", exception.getMessage());
  }

  /**
   * Verify that when an invalid regex pattern is supplied to
   * {@link MetricsInfoImpl#getMeterFilter(String)}, that a PatternSyntaxException is thrown.
   */
  @Test
  public void testIfPatternListInvalid() {
    assertThrows(PatternSyntaxException.class, () -> MetricsInfoImpl.getMeterFilter("[\\]"),
        "Expected an PatternSyntaxException");

  }

  /**
   * Verify that when only one pattern is supplied to {@link MetricsInfoImpl#getMeterFilter(String)}
   * and the resulting filter is given an id whose name matches that pattern, that we get a reply of
   * {@link MeterFilterReply#DENY}.
   */
  @Test
  public void testIfSinglePatternMatchesId() {
    MeterFilter filter = MetricsInfoImpl.getMeterFilter("aaa.*");
    Meter.Id id = new Meter.Id("aaaName", Tags.of("tag", "tag"), null, null, Meter.Type.OTHER);
    MeterFilterReply reply = filter.accept(id);
    assertEquals(MeterFilterReply.DENY, reply, "Expected a reply of DENY");
  }

  /**
   * Verify that when an empty pattern is supplied to
   * {@link MetricsInfoImpl#getMeterFilter(String)}, then no matter what the id is we get a reply of
   * {@link MeterFilterReply#NEUTRAL}.
   */
  @Test
  public void testThatEmptyPatternDoesNotMatchId() {
    MeterFilter filter = MetricsInfoImpl.getMeterFilter("");
    Meter.Id id =
        new Meter.Id("shouldnotmatch", Tags.of("tag", "tag"), null, null, Meter.Type.OTHER);
    MeterFilterReply reply = filter.accept(id);
    assertEquals(MeterFilterReply.NEUTRAL, reply, "Expected a reply of NEUTRAL");
  }

  /**
   * Verify that when only one pattern is supplied to
   * {@link MetricsInfoImpl#getMeterFilter(String)}, and the resulting filter is given an id whose
   * name does not match that pattern, that we get a reply of {@link MeterFilterReply#NEUTRAL}.
   */
  @Test
  public void testIfSinglePatternDoesNotMatch() {
    MeterFilter filter = MetricsInfoImpl.getMeterFilter(("aaa.*"));
    Meter.Id id = new Meter.Id("Wrong", Tags.of("tag", "tag"), null, null, Meter.Type.OTHER);
    MeterFilterReply reply = filter.accept(id);
    assertEquals(MeterFilterReply.NEUTRAL, reply, "Expected a reply of NEUTRAL");
  }

  /**
   * Verify that when multiple patterns are supplied to
   * {@link MetricsInfoImpl#getMeterFilter(String)}, and the resulting filter is given an id whose
   * name matches at least one of the patterns, that we get a reply of
   * {@link MeterFilterReply#DENY}.
   */
  @Test
  public void testIfMultiplePatternsMatches() {
    MeterFilter filter = MetricsInfoImpl.getMeterFilter(("aaa.*,bbb.*,ccc.*"));
    Meter.Id id = new Meter.Id("aaaRight", Tags.of("tag", "tag"), null, null, Meter.Type.OTHER);
    MeterFilterReply reply = filter.accept(id);
    assertEquals(MeterFilterReply.DENY, reply, "Expected a reply of DENY");
  }

  /**
   * Verify that when multiple patterns are supplied to
   * {@link MetricsInfoImpl#getMeterFilter(String)}, and the resulting filter is given an id whose
   * name matches none of the patterns, that we get a reply of {@link MeterFilterReply#NEUTRAL}.
   */
  @Test
  public void testMultiplePatternsWithNoMatch() {
    MeterFilter filter = MetricsInfoImpl.getMeterFilter(("aaa.*,bbb.*,ccc.*"));
    Meter.Id id = new Meter.Id("Wrong", Tags.of("tag", "tag"), null, null, Meter.Type.OTHER);
    MeterFilterReply reply = filter.accept(id);
    assertEquals(MeterFilterReply.NEUTRAL, reply, "Expected a reply of NEUTRAL");
  }
}
