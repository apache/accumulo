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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetricsUtil {

  private final static Pattern camelCasePattern = Pattern.compile("[a-z][A-Z][a-z]");

  /**
   * This method replaces any intended delimiters with the "." delimiter that is used by micrometer.
   * Micrometer will then transform these delimiters to the metric producer's delimiter. Example:
   * "compactorQueue" becomes "compactor.queue" in micrometer. When using Prometheus,
   * "compactor.queue" would become "compactor_queue".
   */
  public static String formatString(String name) {

    // Replace spaces with dot delimiter
    name = name.replace(" ", ".");
    // Replace snake_case with dot delimiter
    name = name.replace("_", ".");
    // Replace hyphens with dot delimiter
    name = name.replace("-", ".");

    // Insert a dot delimiter before each capital letter found in the regex pattern.
    Matcher matcher = camelCasePattern.matcher(name);
    StringBuilder output = new StringBuilder(name);
    int insertCount = 0;
    while (matcher.find()) {
      // Pattern matches on a "aAa" pattern and inserts the dot before the uppercase character.
      // Results in "aAa" becoming "a.Aa".
      output.insert(matcher.start() + 1 + insertCount, ".");
      // The correct index position will shift as inserts occur.
      insertCount++;
    }
    name = output.toString();
    // remove all capital letters after the dot delimiters have been inserted.
    return name.toLowerCase();
  }
}
