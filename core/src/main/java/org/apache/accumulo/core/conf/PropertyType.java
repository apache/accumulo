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

import java.util.regex.Pattern;

import org.apache.accumulo.core.Constants;
import org.apache.hadoop.fs.Path;

public enum PropertyType {
  PREFIX(null, null, null),

  TIMEDURATION("duration", "\\d{1," + Long.toString(Long.MAX_VALUE).length() + "}(?:ms|s|m|h|d)?",
      "A non-negative integer optionally followed by a unit of time (whitespace disallowed), as in 30s.\n"
          + "If no unit of time is specified, seconds are assumed. Valid units are 'ms', 's', 'm', 'h' for milliseconds, seconds, minutes, and hours.\n"
          + "Examples of valid durations are '600', '30s', '45m', '30000ms', '3d', and '1h'.\n"
          + "Examples of invalid durations are '1w', '1h30m', '1s 200ms', 'ms', '', and 'a'.\n"
          + "Unless otherwise stated, the max value for the duration represented in milliseconds is " + Long.MAX_VALUE), DATETIME("date/time",
      "(?:19|20)\\d{12}[A-Z]{3}", "A date/time string in the format: YYYYMMDDhhmmssTTT where TTT is the 3 character time zone"), MEMORY("memory", "\\d{1,"
      + Long.toString(Long.MAX_VALUE).length() + "}(?:B|K|M|G)?",
      "A positive integer optionally followed by a unit of memory (whitespace disallowed), as in 2G.\n"
          + "If no unit is specified, bytes are assumed. Valid units are 'B', 'K', 'M', 'G', for bytes, kilobytes, megabytes, and gigabytes.\n"
          + "Examples of valid memories are '1024', '20B', '100K', '1500M', '2G'.\n"
          + "Examples of invalid memories are '1M500K', '1M 2K', '1MB', '1.5G', '1,024K', '', and 'a'.\n"
          + "Unless otherwise stated, the max value for the memory represented in bytes is " + Long.MAX_VALUE),

  HOSTLIST("host list", "[\\w-]+(?:\\.[\\w-]+)*(?:\\:\\d{1,5})?(?:,[\\w-]+(?:\\.[\\w-]+)*(?:\\:\\d{1,5})?)*",
      "A comma-separated list of hostnames or ip addresses, with optional port numbers.\n"
          + "Examples of valid host lists are 'localhost:2000,www.example.com,10.10.1.1:500' and 'localhost'.\n"
          + "Examples of invalid host lists are '', ':1000', and 'localhost:80000'"),

  PORT("port", "\\d{1,5}", "An positive integer in the range 1024-65535, not already in use or specified elsewhere in the configuration"), COUNT("count",
      "\\d{1,10}", "A non-negative integer in the range of 0-" + Integer.MAX_VALUE), FRACTION("fraction/percentage", "\\d*(?:\\.\\d+)?%?",
      "A floating point number that represents either a fraction or, if suffixed with the '%' character, a percentage.\n"
          + "Examples of valid fractions/percentages are '10', '1000%', '0.05', '5%', '0.2%', '0.0005'.\n"
          + "Examples of invalid fractions/percentages are '', '10 percent', 'Hulk Hogan'"),

  PATH("path", ".*",
      "A string that represents a filesystem path, which can be either relative or absolute to some directory. The filesystem depends on the property. The "
          + "following environment variables will be substituted: " + Constants.PATH_PROPERTY_ENV_VARS), ABSOLUTEPATH("absolute path", null,
      "An absolute filesystem path. The filesystem depends on the property. This is the same as path, but enforces that its root is explicitly specified.") {
    @Override
    public boolean isValidFormat(String value) {
      if (value.trim().equals(""))
        return true;
      return new Path(value).isAbsolute();
    }
  },

  CLASSNAME("java class", "[\\w$.]*", "A fully qualified java class name representing a class on the classpath.\n"
      + "An example is 'java.lang.String', rather than 'String'"),

  STRING("string", ".*",
      "An arbitrary string of characters whose format is unspecified and interpreted based on the context of the property to which it applies."), BOOLEAN(
      "boolean", "(?:true|false)", "Has a value of either 'true' or 'false'"), URI("uri", ".*", "A valid URI");

  private String shortname, format;
  private Pattern regex;

  private PropertyType(String shortname, String regex, String formatDescription) {
    this.shortname = shortname;
    this.format = formatDescription;
    this.regex = regex == null ? null : Pattern.compile(regex, Pattern.DOTALL);
  }

  @Override
  public String toString() {
    return shortname;
  }

  String getFormatDescription() {
    return format;
  }

  public boolean isValidFormat(String value) {
    return (regex == null && value == null) ? true : regex.matcher(value).matches();
  }
}
