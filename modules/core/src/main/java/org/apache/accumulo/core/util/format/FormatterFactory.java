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
package org.apache.accumulo.core.util.format;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FormatterFactory {
  private static final Logger log = LoggerFactory.getLogger(FormatterFactory.class);

  public static Formatter getFormatter(Class<? extends Formatter> formatterClass, Iterable<Entry<Key,Value>> scanner, FormatterConfig config) {
    Formatter formatter = null;
    try {
      formatter = formatterClass.newInstance();
    } catch (Exception e) {
      log.warn("Unable to instantiate formatter. Using default formatter.", e);
      formatter = new DefaultFormatter();
    }
    formatter.initialize(scanner, config);
    return formatter;
  }

  public static Formatter getDefaultFormatter(Iterable<Entry<Key,Value>> scanner, FormatterConfig config) {
    return getFormatter(DefaultFormatter.class, scanner, config);
  }

  private FormatterFactory() {
    // prevent instantiation
  }
}
