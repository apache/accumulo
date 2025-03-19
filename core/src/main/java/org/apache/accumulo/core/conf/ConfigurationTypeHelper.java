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

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationTypeHelper {

  private static final Logger log = LoggerFactory.getLogger(ConfigurationTypeHelper.class);

  /**
   * Interprets a string specifying bytes. A bytes type is specified as a long integer followed by
   * an optional B (bytes), K (KB), M (MB), or G (GB).
   *
   * @param str String value
   * @return interpreted memory size in bytes
   */
  public static long getFixedMemoryAsBytes(String str) {
    char lastChar = str.charAt(str.length() - 1);

    if (lastChar == 'b') {
      log.warn(
          "The 'b' in {} is being considered as bytes. Setting memory by bits is not supported",
          str);
    }
    try {
      int multiplier;
      switch (Character.toUpperCase(lastChar)) {
        case 'G':
          multiplier = 30;
          break;
        case 'M':
          multiplier = 20;
          break;
        case 'K':
          multiplier = 10;
          break;
        case 'B':
          multiplier = 0;
          break;
        default:
          return Long.parseLong(str);
      }
      return Long.parseLong(str.substring(0, str.length() - 1)) << multiplier;
    } catch (Exception ex) {
      throw new IllegalArgumentException(
          "The value '" + str + "' is not a valid memory setting. A valid value would a number "
              + "possibly followed by an optional 'G', 'M', 'K', or 'B'.");
    }
  }

  /**
   * Interprets a string specifying a Memory type which is specified as a long integer followed by
   * an optional B (bytes), K (KB), M (MB), G (GB) or % (percentage).
   *
   * @param str String value
   * @return interpreted memory size in bytes
   */
  public static long getMemoryAsBytes(String str) {
    char lastChar = str.charAt(str.length() - 1);
    if (lastChar == '%') {
      try {
        int percent = Integer.parseInt(str.substring(0, str.length() - 1));
        if (percent <= 0 || percent >= 100) {
          throw new IllegalArgumentException(
              "The value '" + str + "' is not a valid memory setting.");
        }
        return Runtime.getRuntime().maxMemory() * percent / 100;
      } catch (Exception ex) {
        throw new IllegalArgumentException(
            "The value '" + str + "' is not a valid memory setting.");
      }
    }
    return getFixedMemoryAsBytes(str);
  }

  /**
   * Interprets a string specifying a time duration. A time duration is specified as a long integer
   * followed by an optional d (days), h (hours), m (minutes), s (seconds), or ms (milliseconds). A
   * value without a unit is interpreted as seconds.
   *
   * @param str string value
   * @return interpreted time duration in milliseconds
   */
  public static long getTimeInMillis(String str) {
    TimeUnit timeUnit;
    int unitsLen = 1;
    switch (str.charAt(str.length() - 1)) {
      case 'd':
        timeUnit = DAYS;
        break;
      case 'h':
        timeUnit = HOURS;
        break;
      case 'm':
        timeUnit = MINUTES;
        break;
      case 's':
        timeUnit = SECONDS;
        if (str.endsWith("ms")) {
          timeUnit = MILLISECONDS;
          unitsLen = 2;
        }
        break;
      default:
        timeUnit = SECONDS;
        unitsLen = 0;
        break;
    }
    return timeUnit.toMillis(Long.parseLong(str.substring(0, str.length() - unitsLen)));
  }

  /**
   * Interprets a string specifying a fraction. A fraction is specified as a double. An optional %
   * at the end signifies a percentage.
   *
   * @param str string value
   * @return interpreted fraction as a decimal value
   */
  public static double getFraction(String str) {
    if (!str.isEmpty() && str.charAt(str.length() - 1) == '%') {
      return Double.parseDouble(str.substring(0, str.length() - 1)) / 100.0;
    }
    return Double.parseDouble(str);
  }

  // This is not a cache for loaded classes, just a way to avoid spamming the debug log
  private static final Map<String,Class<?>> loaded = Collections.synchronizedMap(new HashMap<>());

  /**
   * Loads a class in the given classloader context, suppressing any exceptions, and optionally
   * providing a default instance to use.
   *
   * @param context the per-table context, can be null
   * @param clazzName the name of the class to load
   * @param base the type of the class
   * @param defaultInstance a default instance if the class cannot be loaded
   * @return a new instance of the class, or the defaultInstance
   */
  public static <T> T getClassInstance(String context, String clazzName, Class<T> base,
      T defaultInstance) {
    T instance = null;

    try {
      instance = getClassInstance(context, clazzName, base);
    } catch (RuntimeException | ReflectiveOperationException e) {
      log.error("Failed to load class {} in classloader context {}", clazzName, context, e);
    }

    if (instance == null) {
      log.info("Using default class ({})",
          defaultInstance == null ? null : defaultInstance.getClass().getName());
      instance = defaultInstance;
    }
    return instance;
  }

  /**
   * Loads a class in the given classloader context.
   *
   * @param context the per-table context, can be null
   * @param clazzName the name of the class to load
   * @param base the type of the class
   * @return a new instance of the class
   */
  public static <T> T getClassInstance(String context, String clazzName, Class<T> base)
      throws ReflectiveOperationException {
    T instance;

    Class<? extends T> clazz = ClassLoaderUtil.loadClass(context, clazzName, base);
    instance = clazz.getDeclaredConstructor().newInstance();
    if (loaded.put(clazzName, clazz) != clazz) {
      log.debug("Loaded class : {}", clazzName);
    }

    return instance;
  }

  /**
   * Get the number of threads from string property. If the value ends with C, then it will be
   * multiplied by the number of cores.
   */
  public static int getNumThreads(String threads) {
    if (threads == null) {
      threads = ClientProperty.BULK_LOAD_THREADS.getDefaultValue();
    }
    int nThreads;
    if (threads.toUpperCase().endsWith("C")) {
      nThreads = Runtime.getRuntime().availableProcessors()
          * Integer.parseInt(threads.substring(0, threads.length() - 1));
    } else {
      nThreads = Integer.parseInt(threads);
    }
    return nThreads;
  }

  /**
   * Get the set of volumes parsed from a volumes property type, and throw exceptions if the volumes
   * aren't valid, are null, contains only blanks, or contains duplicates. An empty string is
   * allowed (resulting in an empty set of volumes), to handle the case where the property is not
   * set by a user (or... is set to the same as the default, which is equivalent to not being set).
   * If the property is required to be set, it is the caller's responsibility to verify that the
   * parsed set is non-empty.
   *
   * @throws IllegalArgumentException when the volumes are set to something that cannot be parsed
   */
  public static Set<String> getVolumeUris(String volumes) {
    if (requireNonNull(volumes).isEmpty()) {
      // special case when the property is not set and defaults to an empty string
      return Set.of();
    }
    var blanksRemoved = Arrays.stream(volumes.split(",")).map(String::strip)
        .filter(Predicate.not(String::isEmpty)).collect(Collectors.toList());
    if (blanksRemoved.isEmpty()) {
      throw new IllegalArgumentException("property contains only blank volumes");
    }
    var deduplicated = blanksRemoved.stream().map(ConfigurationTypeHelper::normalizeVolume)
        .collect(Collectors.toCollection(LinkedHashSet::new));
    if (deduplicated.size() < blanksRemoved.size()) {
      throw new IllegalArgumentException("property contains duplicate volumes");
    }
    return deduplicated;
  }

  private static String normalizeVolume(String volume) {
    if (!volume.contains(":")) {
      throw new IllegalArgumentException("'" + volume + "' is not a fully qualified URI");
    }
    try {
      // pass through URI to unescape hex encoded chars (e.g. convert %2C to "," char)
      return new Path(new URI(volume.strip())).toString();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "volume contains '" + volume + "' which has a syntax error", e);
    }
  }

}
