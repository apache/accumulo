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
package org.apache.accumulo.core.util.format;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FormatterFactoryTest {

  Iterable<Entry<Key,Value>> scanner;

  @BeforeEach
  public void setUp() {
    scanner = Collections.<Key,Value>emptyMap().entrySet();
  }

  @Test
  public void testGetDefaultFormatter() {
    final FormatterConfig timestampConfig = new FormatterConfig().setPrintTimestamps(true);
    Formatter defaultFormatter = FormatterFactory.getDefaultFormatter(scanner, timestampConfig);
    Formatter bogusFormatter =
        FormatterFactory.getFormatter(Formatter.class, scanner, timestampConfig);
    assertEquals(defaultFormatter.getClass(), bogusFormatter.getClass());
  }

}
