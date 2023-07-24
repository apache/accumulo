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
package org.apache.accumulo.core.metrics;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsUtilTest {

  private static final Logger log = LoggerFactory.getLogger(MetricsUtilTest.class);

  @Test
  public void testLabelFormatting() {
    Map.of("camelCase", "camel.case", "camelCamelCamelCase", "camel.camel.camel.case", "snake_case",
        "snake.case", "normal.label", "normal.label", "space separated", "space.separated",
        "Capital", "capital", "hyphen-ated", "hyphen.ated").forEach((label, correctFormat) -> {
          log.info("Testing Label: {}", label);
          String output = MetricsUtil.formatString(label);
          assertTrue(output.contentEquals(correctFormat));
        });
  }
}
