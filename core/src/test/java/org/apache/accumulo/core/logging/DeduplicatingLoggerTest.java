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
package org.apache.accumulo.core.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.StringWriter;
import java.time.Duration;

import org.apache.accumulo.core.logging.ConditionalLogger.DeduplicatingLogger;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeduplicatingLoggerTest {

  private static final Logger LOG = LoggerFactory.getLogger(DeduplicatingLoggerTest.class);
  private static final Logger TEST_LOGGER =
      new DeduplicatingLogger(LOG, Duration.ofMinutes(1), 100);

  @Test
  public void test() {

    StringWriter writer = new StringWriter();

    // Programatically modify the Log4j2 Logging configuration to add an appender
    LoggerContext ctx = LoggerContext.getContext(false);
    Configuration cfg = ctx.getConfiguration();
    PatternLayout layout = PatternLayout.createDefaultLayout(cfg);
    Appender appender = WriterAppender.createAppender(layout, null, writer,
        "DeduplicatingLoggerTestAppender", false, true);
    appender.start();
    cfg.addAppender(appender);
    cfg.getLoggerConfig(DeduplicatingLoggerTest.class.getName()).addAppender(appender, null, null);

    TEST_LOGGER.error("ERROR TEST");
    TEST_LOGGER.warn("WARN TEST");
    assertEquals(1, StringUtils.countMatches(writer.toString(), "ERROR TEST"));
    assertEquals(1, StringUtils.countMatches(writer.toString(), "WARN TEST"));
    TEST_LOGGER.error("ERROR TEST");
    TEST_LOGGER.warn("WARN TEST");
    assertEquals(1, StringUtils.countMatches(writer.toString(), "ERROR TEST"));
    assertEquals(1, StringUtils.countMatches(writer.toString(), "WARN TEST"));

  }

}
