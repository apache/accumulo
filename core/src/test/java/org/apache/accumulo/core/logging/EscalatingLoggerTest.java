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

import org.apache.accumulo.core.logging.ConditionalLogger.EscalatingLogger;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class EscalatingLoggerTest {

  private static final Logger LOG = LoggerFactory.getLogger(EscalatingLoggerTest.class);
  private static final Logger TEST_LOGGER =
      new EscalatingLogger(LOG, Duration.ofSeconds(3), 100, Level.WARN);

  @Test
  public void test() throws InterruptedException {

    StringWriter writer = new StringWriter();

    // Programatically modify the Log4j2 Logging configuration to add an appender
    LoggerContext ctx = LoggerContext.getContext(false);
    Configuration cfg = ctx.getConfiguration();
    PatternLayout layout = PatternLayout.newBuilder().withConfiguration(cfg)
        .withPattern(PatternLayout.SIMPLE_CONVERSION_PATTERN).build();
    Appender appender = WriterAppender.createAppender(layout, null, writer,
        "EscalatingLoggerTestAppender", false, true);
    appender.start();
    cfg.addAppender(appender);
    cfg.getLoggerConfig(EscalatingLoggerTest.class.getName()).addAppender(appender, null, null);

    TEST_LOGGER.info("TEST MESSAGE");
    TEST_LOGGER.info("TEST MESSAGE");
    TEST_LOGGER.info("TEST MESSAGE");
    TEST_LOGGER.info("TEST MESSAGE");

    assertEquals(1, StringUtils.countMatches(writer.toString(), "WARN"));
    assertEquals(3, StringUtils.countMatches(writer.toString(), "INFO"));

    Thread.sleep(5000);

    TEST_LOGGER.info("TEST MESSAGE");

    assertEquals(2, StringUtils.countMatches(writer.toString(), "WARN"));
    assertEquals(3, StringUtils.countMatches(writer.toString(), "INFO"));

  }

}
