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

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiPredicate;

import org.apache.accumulo.core.util.Pair;
import org.slf4j.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Logger that wraps another Logger and only emits a log message once per the supplied duration.
 *
 */
public abstract class ConditionalLogger {

  /**
   * A Logger implementation that will log a message at the supplied elevated level if it has not
   * been seen in the supplied duration. For repeat occurrences the message will be logged at the
   * level used in code (which is likely a lower level). Note that the first log message will be
   * logged at the elevated level because it has not been seen before.
   */
  public static class EscalatingLogger extends DeduplicatingLogger {

    private final ConditionalLogAction elevatedLogAction;

    public EscalatingLogger(Logger log, Duration threshold, long maxCachedLogMessages,
        ConditionalLogAction elevatedLogAction) {
      super(log, threshold, maxCachedLogMessages);
      this.elevatedLogAction = requireNonNull(elevatedLogAction);
    }

    @Override
    public void trace(String format, Object... arguments) {
      log(elevatedLogAction, Logger::trace, format, arguments);
    }

    @Override
    public void debug(String format, Object... arguments) {
      log(elevatedLogAction, Logger::debug, format, arguments);
    }

    @Override
    public void info(String format, Object... arguments) {
      log(elevatedLogAction, Logger::info, format, arguments);
    }

    @Override
    public void warn(String format, Object... arguments) {
      log(elevatedLogAction, Logger::warn, format, arguments);
    }

    @Override
    public void error(String format, Object... arguments) {
      log(elevatedLogAction, Logger::error, format, arguments);
    }

  }

  /**
   * A Logger implementation that will suppress duplicate messages within the supplied duration.
   */
  public static class DeduplicatingLogger extends ConditionalLogger {

    public DeduplicatingLogger(Logger log, Duration threshold, long maxCachedLogMessages) {
      super(log, new BiPredicate<>() {

        private final Cache<Pair<String,List<Object>>,Boolean> cache = Caffeine.newBuilder()
            .expireAfterWrite(threshold).maximumSize(maxCachedLogMessages).build();

        private final ConcurrentMap<Pair<String,List<Object>>,Boolean> cacheMap = cache.asMap();

        /**
         * Function that will return true if the message with the provided arguments (minus any
         * included Throwable as the last argument) has not been seen in the supplied duration.
         * Deduplication will only work if the arguments are of types that implement meaningful
         * equals. This is not generally true of Throwables.
         *
         * @param msg log message
         * @param args log message arguments
         * @return true if message has not been seen in duration, else false.
         */
        @Override
        public boolean test(String msg, List<Object> args) {
          if (!args.isEmpty() && args.get(args.size() - 1) instanceof Throwable) {
            args = args.subList(0, args.size() - 1);
          }
          return cacheMap.putIfAbsent(new Pair<>(msg, args), true) == null;
        }

      });
    }

  }

  protected final Logger delegate;
  protected final BiPredicate<String,List<Object>> condition;

  protected ConditionalLogger(Logger log, BiPredicate<String,List<Object>> condition) {
    this.delegate = requireNonNull(log);
    this.condition = requireNonNull(condition);
  }

  @FunctionalInterface
  public interface ConditionalLogAction {
    void log(Logger logger, String format, Object... arguments);
  }

  /**
   * Conditionally executes the log action with the provided format string and arguments
   *
   * @param conditionTrueLogAction the log action to execute (e.g. Logger::warn, Logger::debug,
   *        etc.) when the condition is true (optional, may be null)
   * @param conditionFalseLogAction the log action to execute (e.g. Logger::warn, Logger::debug,
   *        etc.) when the condition is false (optional, may be null)
   * @param format the message format String for the logger
   * @param arguments the arguments to the format String
   */
  protected final void log(ConditionalLogAction conditionTrueLogAction,
      ConditionalLogAction conditionFalseLogAction, String format, Object... arguments) {
    if (arguments == null) {
      arguments = new Object[0];
    }
    if (condition.test(format, Arrays.asList(arguments))) {
      if (conditionTrueLogAction != null) {
        conditionTrueLogAction.log(delegate, format, arguments);
      }
    } else if (conditionFalseLogAction != null) {
      conditionFalseLogAction.log(delegate, format, arguments);
    }
  }

  public void trace(String format, Object... arguments) {
    log(Logger::trace, null, format, arguments);
  }

  public void debug(String format, Object... arguments) {
    log(Logger::debug, null, format, arguments);
  }

  public void info(String format, Object... arguments) {
    log(Logger::info, null, format, arguments);
  }

  public void warn(String format, Object... arguments) {
    log(Logger::warn, null, format, arguments);
  }

  public void error(String format, Object... arguments) {
    log(Logger::error, null, format, arguments);
  }

}
