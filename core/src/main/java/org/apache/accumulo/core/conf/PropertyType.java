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

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.Constants;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;

/**
 * Types of {@link Property} values. Each type has a short name, a description, and a regex which valid values match. All of these fields are optional.
 */
public enum PropertyType {
  PREFIX(null, Predicates.<String> alwaysFalse(), null),

  TIMEDURATION("duration", boundedUnits(0, Long.MAX_VALUE, true, "", "ms", "s", "m", "h", "d"),
      "A non-negative integer optionally followed by a unit of time (whitespace disallowed), as in 30s.\n"
          + "If no unit of time is specified, seconds are assumed. Valid units are 'ms', 's', 'm', 'h' for milliseconds, seconds, minutes, and hours.\n"
          + "Examples of valid durations are '600', '30s', '45m', '30000ms', '3d', and '1h'.\n"
          + "Examples of invalid durations are '1w', '1h30m', '1s 200ms', 'ms', '', and 'a'.\n"
          + "Unless otherwise stated, the max value for the duration represented in milliseconds is " + Long.MAX_VALUE),

  MEMORY("memory", boundedUnits(0, Long.MAX_VALUE, false, "", "B", "K", "M", "G"),
      "A positive integer optionally followed by a unit of memory (whitespace disallowed), as in 2G.\n"
          + "If no unit is specified, bytes are assumed. Valid units are 'B', 'K', 'M', 'G', for bytes, kilobytes, megabytes, and gigabytes.\n"
          + "Examples of valid memories are '1024', '20B', '100K', '1500M', '2G'.\n"
          + "Examples of invalid memories are '1M500K', '1M 2K', '1MB', '1.5G', '1,024K', '', and 'a'.\n"
          + "Unless otherwise stated, the max value for the memory represented in bytes is " + Long.MAX_VALUE),

  HOSTLIST("host list", new Matches("[\\w-]+(?:\\.[\\w-]+)*(?:\\:\\d{1,5})?(?:,[\\w-]+(?:\\.[\\w-]+)*(?:\\:\\d{1,5})?)*"),
      "A comma-separated list of hostnames or ip addresses, with optional port numbers.\n"
          + "Examples of valid host lists are 'localhost:2000,www.example.com,10.10.1.1:500' and 'localhost'.\n"
          + "Examples of invalid host lists are '', ':1000', and 'localhost:80000'"),

  PORT("port", Predicates.or(new Bounds(1024, 65535), in(true, "0")),
      "An positive integer in the range 1024-65535, not already in use or specified elsewhere in the configuration"),

  COUNT("count", new Bounds(0, Integer.MAX_VALUE), "A non-negative integer in the range of 0-" + Integer.MAX_VALUE),

  FRACTION("fraction/percentage", new FractionPredicate(),
      "A floating point number that represents either a fraction or, if suffixed with the '%' character, a percentage.\n"
          + "Examples of valid fractions/percentages are '10', '1000%', '0.05', '5%', '0.2%', '0.0005'.\n"
          + "Examples of invalid fractions/percentages are '', '10 percent', 'Hulk Hogan'"),

  PATH("path", Predicates.<String> alwaysTrue(),
      "A string that represents a filesystem path, which can be either relative or absolute to some directory. The filesystem depends on the property. The "
          + "following environment variables will be substituted: " + Constants.PATH_PROPERTY_ENV_VARS),

  ABSOLUTEPATH("absolute path", new Predicate<String>() {
    @Override
    public boolean apply(final String input) {
      return input == null || input.trim().isEmpty() || new Path(input.trim()).isAbsolute();
    }
  }, "An absolute filesystem path. The filesystem depends on the property. This is the same as path, but enforces that its root is explicitly specified."),

  CLASSNAME("java class", new Matches("[\\w$.]*"), "A fully qualified java class name representing a class on the classpath.\n"
      + "An example is 'java.lang.String', rather than 'String'"),

  CLASSNAMELIST("java class list", new Matches("[\\w$.,]*"), "A list of fully qualified java class names representing classes on the classpath.\n"
      + "An example is 'java.lang.String', rather than 'String'"),

  DURABILITY("durability", in(true, null, "none", "log", "flush", "sync"), "One of 'none', 'log', 'flush' or 'sync'."),

  STRING("string", Predicates.<String> alwaysTrue(),
      "An arbitrary string of characters whose format is unspecified and interpreted based on the context of the property to which it applies."),

  BOOLEAN("boolean", in(false, null, "true", "false"), "Has a value of either 'true' or 'false' (case-insensitive)"),

  URI("uri", Predicates.<String> alwaysTrue(), "A valid URI");

  private String shortname, format;
  private Predicate<String> predicate;

  private PropertyType(String shortname, Predicate<String> predicate, String formatDescription) {
    this.shortname = shortname;
    this.predicate = predicate;
    this.format = formatDescription;
  }

  @Override
  public String toString() {
    return shortname;
  }

  /**
   * Gets the description of this type.
   *
   * @return description
   */
  String getFormatDescription() {
    return format;
  }

  /**
   * Checks if the given value is valid for this type.
   *
   * @return true if value is valid or null, or if this type has no regex
   */
  public boolean isValidFormat(String value) {
    return predicate.apply(value);
  }

  private static Predicate<String> in(final boolean caseSensitive, final String... strings) {
    List<String> allowedSet = Arrays.asList(strings);
    if (caseSensitive) {
      return Predicates.in(allowedSet);
    } else {
      Function<String,String> toLower = new Function<String,String>() {
        @Override
        public String apply(final String input) {
          return input == null ? null : input.toLowerCase();
        }
      };
      return Predicates.compose(Predicates.in(Collections2.transform(allowedSet, toLower)), toLower);
    }
  }

  private static Predicate<String> boundedUnits(final long lowerBound, final long upperBound, final boolean caseSensitive, final String... suffixes) {
    return Predicates.or(Predicates.isNull(),
        Predicates.and(new HasSuffix(caseSensitive, suffixes), Predicates.compose(new Bounds(lowerBound, upperBound), new StripUnits())));
  }

  private static class StripUnits implements Function<String,String> {
    private static Pattern SUFFIX_REGEX = Pattern.compile("[^\\d]*$");

    @Override
    public String apply(final String input) {
      Preconditions.checkNotNull(input);
      return SUFFIX_REGEX.matcher(input.trim()).replaceAll("");
    }
  }

  private static class HasSuffix implements Predicate<String> {

    private final Predicate<String> p;

    public HasSuffix(final boolean caseSensitive, final String... suffixes) {
      p = in(caseSensitive, suffixes);
    }

    @Override
    public boolean apply(final String input) {
      Preconditions.checkNotNull(input);
      Matcher m = StripUnits.SUFFIX_REGEX.matcher(input);
      if (m.find()) {
        if (m.groupCount() != 0) {
          throw new AssertionError(m.groupCount());
        }
        return p.apply(m.group());
      } else {
        return true;
      }
    }
  }

  private static class FractionPredicate implements Predicate<String> {
    @Override
    public boolean apply(final String input) {
      if (input == null) {
        return true;
      }
      try {
        double d;
        if (input.length() > 0 && input.charAt(input.length() - 1) == '%') {
          d = Double.parseDouble(input.substring(0, input.length() - 1));
        } else {
          d = Double.parseDouble(input);
        }
        return d >= 0;
      } catch (NumberFormatException e) {
        return false;
      }
    }
  }

  private static class Bounds implements Predicate<String> {

    private final long lowerBound, upperBound;
    private final boolean lowerInclusive, upperInclusive;

    public Bounds(final long lowerBound, final long upperBound) {
      this(lowerBound, true, upperBound, true);
    }

    public Bounds(final long lowerBound, final boolean lowerInclusive, final long upperBound, final boolean upperInclusive) {
      this.lowerBound = lowerBound;
      this.lowerInclusive = lowerInclusive;
      this.upperBound = upperBound;
      this.upperInclusive = upperInclusive;
    }

    @Override
    public boolean apply(final String input) {
      if (input == null) {
        return true;
      }
      long number;
      try {
        number = Long.parseLong(input);
      } catch (NumberFormatException e) {
        return false;
      }
      if (number < lowerBound || (!lowerInclusive && number == lowerBound)) {
        return false;
      }
      if (number > upperBound || (!upperInclusive && number == upperBound)) {
        return false;
      }
      return true;
    }

  }

  private static class Matches implements Predicate<String> {

    private final Pattern pattern;

    public Matches(final String pattern) {
      this(pattern, Pattern.DOTALL);
    }

    public Matches(final String pattern, int flags) {
      this(Pattern.compile(Preconditions.checkNotNull(pattern), flags));
    }

    public Matches(final Pattern pattern) {
      Preconditions.checkNotNull(pattern);
      this.pattern = pattern;
    }

    @Override
    public boolean apply(final String input) {
      // TODO when the input is null, it just means that the property wasn't set
      // we can add checks for not null for required properties with Predicates.and(Predicates.notNull(), ...),
      // or we can stop assuming that null is always okay for a Matches predicate, and do that explicitly with Predicates.or(Predicates.isNull(), ...)
      return input == null || pattern.matcher(input).matches();
    }

  }

}
