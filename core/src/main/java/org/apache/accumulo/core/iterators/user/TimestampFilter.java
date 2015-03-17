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
package org.apache.accumulo.core.iterators.user;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * A Filter that matches entries whose timestamps fall within a range.
 */
public class TimestampFilter extends Filter {
  private static final String LONG_PREFIX = "LONG";
  private final SimpleDateFormat dateParser = initDateParser();

  private static SimpleDateFormat initDateParser() {
    SimpleDateFormat dateParser = new SimpleDateFormat("yyyyMMddHHmmssz");
    dateParser.setTimeZone(TimeZone.getTimeZone("GMT"));
    return dateParser;
  }

  public static final String START = "start";
  public static final String START_INCL = "startInclusive";
  public static final String END = "end";
  public static final String END_INCL = "endInclusive";
  private long start;
  private long end;
  private boolean startInclusive;
  private boolean endInclusive;
  private boolean hasStart;
  private boolean hasEnd;

  public TimestampFilter() {}

  @Override
  public boolean accept(Key k, Value v) {
    long ts = k.getTimestamp();
    if ((hasStart && (ts < start)) || (hasEnd && (ts > end)))
      return false;
    if (hasStart && !startInclusive && ts == start)
      return false;
    if (hasEnd && !endInclusive && ts == end)
      return false;
    return true;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    if (options == null)
      throw new IllegalArgumentException("start and/or end must be set for " + TimestampFilter.class.getName());

    super.init(source, options, env);

    hasStart = false;
    hasEnd = false;
    startInclusive = true;
    endInclusive = true;

    if (options.containsKey(START))
      hasStart = true;
    if (options.containsKey(END))
      hasEnd = true;
    if (!hasStart && !hasEnd)
      throw new IllegalArgumentException("must have either start or end for " + TimestampFilter.class.getName());

    try {
      if (hasStart) {
        String s = options.get(START);
        if (s.startsWith(LONG_PREFIX))
          start = Long.parseLong(s.substring(LONG_PREFIX.length()));
        else
          start = dateParser.parse(s).getTime();
      }
      if (hasEnd) {
        String s = options.get(END);
        if (s.startsWith(LONG_PREFIX))
          end = Long.parseLong(s.substring(LONG_PREFIX.length()));
        else
          end = dateParser.parse(s).getTime();
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
    if (options.get(START_INCL) != null)
      startInclusive = Boolean.parseBoolean(options.get(START_INCL));
    if (options.get(END_INCL) != null)
      endInclusive = Boolean.parseBoolean(options.get(END_INCL));
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    TimestampFilter copy = (TimestampFilter) super.deepCopy(env);
    copy.hasStart = hasStart;
    copy.start = start;
    copy.startInclusive = startInclusive;
    copy.hasEnd = hasEnd;
    copy.end = end;
    copy.endInclusive = endInclusive;
    return copy;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("tsfilter");
    io.setDescription("TimestampFilter displays entries with timestamps between specified values");
    io.addNamedOption("start", "start timestamp (yyyyMMddHHmmssz or LONG<longstring>)");
    io.addNamedOption("end", "end timestamp (yyyyMMddHHmmssz or LONG<longstring>)");
    io.addNamedOption("startInclusive", "true or false");
    io.addNamedOption("endInclusive", "true or false");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (super.validateOptions(options) == false)
      return false;
    boolean hasStart = false;
    boolean hasEnd = false;
    try {
      if (options.containsKey(START)) {
        hasStart = true;
        String s = options.get(START);
        if (s.startsWith(LONG_PREFIX))
          Long.valueOf(s.substring(LONG_PREFIX.length()));
        else
          dateParser.parse(s);
      }
      if (options.containsKey(END)) {
        hasEnd = true;
        String s = options.get(END);
        if (s.startsWith(LONG_PREFIX))
          Long.valueOf(s.substring(LONG_PREFIX.length()));
        else
          dateParser.parse(s);
      }
      if (!hasStart && !hasEnd)
        throw new IllegalArgumentException(START + " or " + END + " must be specified");
      if (options.get(START_INCL) != null)
        Boolean.parseBoolean(options.get(START_INCL));
      if (options.get(END_INCL) != null)
        Boolean.parseBoolean(options.get(END_INCL));
    } catch (Exception e) {
      throw new IllegalArgumentException("invalid options", e);
    }
    return true;
  }

  /**
   * A convenience method for setting the range of timestamps accepted by the timestamp filter.
   *
   * @param is
   *          the iterator setting object to configure
   * @param start
   *          the start timestamp, inclusive (yyyyMMddHHmmssz)
   * @param end
   *          the end timestamp, inclusive (yyyyMMddHHmmssz)
   */
  public static void setRange(IteratorSetting is, String start, String end) {
    setRange(is, start, true, end, true);
  }

  /**
   * A convenience method for setting the range of timestamps accepted by the timestamp filter.
   *
   * @param is
   *          the iterator setting object to configure
   * @param start
   *          the start timestamp (yyyyMMddHHmmssz)
   * @param startInclusive
   *          boolean indicating whether the start is inclusive
   * @param end
   *          the end timestamp (yyyyMMddHHmmssz)
   * @param endInclusive
   *          boolean indicating whether the end is inclusive
   */
  public static void setRange(IteratorSetting is, String start, boolean startInclusive, String end, boolean endInclusive) {
    setStart(is, start, startInclusive);
    setEnd(is, end, endInclusive);
  }

  /**
   * A convenience method for setting the start timestamp accepted by the timestamp filter.
   *
   * @param is
   *          the iterator setting object to configure
   * @param start
   *          the start timestamp (yyyyMMddHHmmssz)
   * @param startInclusive
   *          boolean indicating whether the start is inclusive
   */
  public static void setStart(IteratorSetting is, String start, boolean startInclusive) {
    SimpleDateFormat dateParser = initDateParser();
    try {
      long startTS = dateParser.parse(start).getTime();
      setStart(is, startTS, startInclusive);
    } catch (ParseException e) {
      throw new IllegalArgumentException("couldn't parse " + start);
    }
  }

  /**
   * A convenience method for setting the end timestamp accepted by the timestamp filter.
   *
   * @param is
   *          the iterator setting object to configure
   * @param end
   *          the end timestamp (yyyyMMddHHmmssz)
   * @param endInclusive
   *          boolean indicating whether the end is inclusive
   */
  public static void setEnd(IteratorSetting is, String end, boolean endInclusive) {
    SimpleDateFormat dateParser = initDateParser();
    try {
      long endTS = dateParser.parse(end).getTime();
      setEnd(is, endTS, endInclusive);
    } catch (ParseException e) {
      throw new IllegalArgumentException("couldn't parse " + end);
    }
  }

  /**
   * A convenience method for setting the range of timestamps accepted by the timestamp filter.
   *
   * @param is
   *          the iterator setting object to configure
   * @param start
   *          the start timestamp, inclusive
   * @param end
   *          the end timestamp, inclusive
   */
  public static void setRange(IteratorSetting is, long start, long end) {
    setRange(is, start, true, end, true);
  }

  /**
   * A convenience method for setting the range of timestamps accepted by the timestamp filter.
   *
   * @param is
   *          the iterator setting object to configure
   * @param start
   *          the start timestamp
   * @param startInclusive
   *          boolean indicating whether the start is inclusive
   * @param end
   *          the end timestamp
   * @param endInclusive
   *          boolean indicating whether the end is inclusive
   */
  public static void setRange(IteratorSetting is, long start, boolean startInclusive, long end, boolean endInclusive) {
    setStart(is, start, startInclusive);
    setEnd(is, end, endInclusive);
  }

  /**
   * A convenience method for setting the start timestamp accepted by the timestamp filter.
   *
   * @param is
   *          the iterator setting object to configure
   * @param start
   *          the start timestamp
   * @param startInclusive
   *          boolean indicating whether the start is inclusive
   */
  public static void setStart(IteratorSetting is, long start, boolean startInclusive) {
    is.addOption(START, LONG_PREFIX + Long.toString(start));
    is.addOption(START_INCL, Boolean.toString(startInclusive));
  }

  /**
   * A convenience method for setting the end timestamp accepted by the timestamp filter.
   *
   * @param is
   *          the iterator setting object to configure
   * @param end
   *          the end timestamp
   * @param endInclusive
   *          boolean indicating whether the end is inclusive
   */
  public static void setEnd(IteratorSetting is, long end, boolean endInclusive) {
    is.addOption(END, LONG_PREFIX + Long.toString(end));
    is.addOption(END_INCL, Boolean.toString(endInclusive));
  }
}
