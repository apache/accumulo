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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * A Filter that matches entries based on Java regular expressions.
 */
public class RegExFilter extends Filter {

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    RegExFilter result = (RegExFilter) super.deepCopy(env);
    result.rowMatcher = copyMatcher(rowMatcher);
    result.colfMatcher = copyMatcher(colfMatcher);
    result.colqMatcher = copyMatcher(colqMatcher);
    result.valueMatcher = copyMatcher(valueMatcher);
    result.orFields = orFields;
    return result;
  }

  public static final String ROW_REGEX = "rowRegex";
  public static final String COLF_REGEX = "colfRegex";
  public static final String COLQ_REGEX = "colqRegex";
  public static final String VALUE_REGEX = "valueRegex";
  public static final String OR_FIELDS = "orFields";
  public static final String ENCODING = "encoding";
  public static final String MATCH_SUBSTRING = "matchSubstring";

  public static final String ENCODING_DEFAULT = UTF_8.name();

  private Matcher rowMatcher;
  private Matcher colfMatcher;
  private Matcher colqMatcher;
  private Matcher valueMatcher;
  private boolean orFields = false;
  private boolean matchSubstring = false;

  private Charset encoding = Charset.forName(ENCODING_DEFAULT);

  private Matcher copyMatcher(Matcher m) {
    if (m == null)
      return m;
    else
      return m.pattern().matcher("");
  }

  private boolean matches(Matcher matcher, ByteSequence bs) {
    if (matcher != null) {
      matcher.reset(new String(bs.getBackingArray(), bs.offset(), bs.length(), encoding));
      return matchSubstring ? matcher.find() : matcher.matches();
    }
    return !orFields;
  }

  private boolean matches(Matcher matcher, byte data[], int offset, int len) {
    if (matcher != null) {
      matcher.reset(new String(data, offset, len, encoding));
      return matchSubstring ? matcher.find() : matcher.matches();
    }
    return !orFields;
  }

  @Override
  public boolean accept(Key key, Value value) {
    if (orFields)
      return ((matches(rowMatcher, rowMatcher == null ? null : key.getRowData()))
          || (matches(colfMatcher, colfMatcher == null ? null : key.getColumnFamilyData()))
          || (matches(colqMatcher, colqMatcher == null ? null : key.getColumnQualifierData())) || (matches(valueMatcher, value.get(), 0, value.get().length)));
    return ((matches(rowMatcher, rowMatcher == null ? null : key.getRowData()))
        && (matches(colfMatcher, colfMatcher == null ? null : key.getColumnFamilyData()))
        && (matches(colqMatcher, colqMatcher == null ? null : key.getColumnQualifierData())) && (matches(valueMatcher, value.get(), 0, value.get().length)));
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (options.containsKey(ROW_REGEX)) {
      rowMatcher = Pattern.compile(options.get(ROW_REGEX)).matcher("");
    } else {
      rowMatcher = null;
    }

    if (options.containsKey(COLF_REGEX)) {
      colfMatcher = Pattern.compile(options.get(COLF_REGEX)).matcher("");
    } else {
      colfMatcher = null;
    }

    if (options.containsKey(COLQ_REGEX)) {
      colqMatcher = Pattern.compile(options.get(COLQ_REGEX)).matcher("");
    } else {
      colqMatcher = null;
    }

    if (options.containsKey(VALUE_REGEX)) {
      valueMatcher = Pattern.compile(options.get(VALUE_REGEX)).matcher("");
    } else {
      valueMatcher = null;
    }

    if (options.containsKey(OR_FIELDS)) {
      orFields = Boolean.parseBoolean(options.get(OR_FIELDS));
    } else {
      orFields = false;
    }

    if (options.containsKey(MATCH_SUBSTRING)) {
      matchSubstring = Boolean.parseBoolean(options.get(MATCH_SUBSTRING));
    } else {
      matchSubstring = false;
    }

    if (options.containsKey(ENCODING)) {
      encoding = Charset.forName(options.get(ENCODING));
    }
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("regex");
    io.setDescription("The RegExFilter/Iterator allows you to filter for key/value pairs based on regular expressions");
    io.addNamedOption(RegExFilter.ROW_REGEX, "regular expression on row");
    io.addNamedOption(RegExFilter.COLF_REGEX, "regular expression on column family");
    io.addNamedOption(RegExFilter.COLQ_REGEX, "regular expression on column qualifier");
    io.addNamedOption(RegExFilter.VALUE_REGEX, "regular expression on value");
    io.addNamedOption(RegExFilter.OR_FIELDS, "use OR instead of AND when multiple regexes given");
    io.addNamedOption(RegExFilter.MATCH_SUBSTRING, "match on substrings");
    io.addNamedOption(RegExFilter.ENCODING, "character encoding of byte array value (default is " + ENCODING_DEFAULT + ")");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (super.validateOptions(options) == false)
      return false;

    try {
      if (options.containsKey(ROW_REGEX))
        Pattern.compile(options.get(ROW_REGEX)).matcher("");

      if (options.containsKey(COLF_REGEX))
        Pattern.compile(options.get(COLF_REGEX)).matcher("");

      if (options.containsKey(COLQ_REGEX))
        Pattern.compile(options.get(COLQ_REGEX)).matcher("");

      if (options.containsKey(VALUE_REGEX))
        Pattern.compile(options.get(VALUE_REGEX)).matcher("");
    } catch (Exception e) {
      throw new IllegalArgumentException("bad regex", e);
    }

    if (options.containsKey(ENCODING)) {
      try {
        String encodingOpt = options.get(ENCODING);
        this.encoding = Charset.forName(encodingOpt.isEmpty() ? ENCODING_DEFAULT : encodingOpt);
      } catch (UnsupportedCharsetException e) {
        throw new IllegalArgumentException("invalid encoding " + ENCODING + ":" + this.encoding, e);
      }
    }

    return true;
  }

  /**
   * Encode the terms to match against in the iterator. Same as calling {@link #setRegexs(IteratorSetting, String, String, String, String, boolean, boolean)}
   * with matchSubstring set to false
   *
   * @param si
   *          ScanIterator config to be updated
   * @param rowTerm
   *          the pattern to match against the Key's row. Not used if null.
   * @param cfTerm
   *          the pattern to match against the Key's column family. Not used if null.
   * @param cqTerm
   *          the pattern to match against the Key's column qualifier. Not used if null.
   * @param valueTerm
   *          the pattern to match against the Key's value. Not used if null.
   * @param orFields
   *          if true, any of the non-null terms can match to return the entry
   */
  public static void setRegexs(IteratorSetting si, String rowTerm, String cfTerm, String cqTerm, String valueTerm, boolean orFields) {
    setRegexs(si, rowTerm, cfTerm, cqTerm, valueTerm, orFields, false);
  }

  /**
   * Encode the terms to match against in the iterator
   *
   * @param si
   *          ScanIterator config to be updated
   * @param rowTerm
   *          the pattern to match against the Key's row. Not used if null.
   * @param cfTerm
   *          the pattern to match against the Key's column family. Not used if null.
   * @param cqTerm
   *          the pattern to match against the Key's column qualifier. Not used if null.
   * @param valueTerm
   *          the pattern to match against the Key's value. Not used if null.
   * @param matchSubstring
   *          if true then search expressions will match on partial strings
   */
  public static void setRegexs(IteratorSetting si, String rowTerm, String cfTerm, String cqTerm, String valueTerm, boolean orFields, boolean matchSubstring) {

    if (rowTerm != null)
      si.addOption(RegExFilter.ROW_REGEX, rowTerm);
    if (cfTerm != null)
      si.addOption(RegExFilter.COLF_REGEX, cfTerm);
    if (cqTerm != null)
      si.addOption(RegExFilter.COLQ_REGEX, cqTerm);
    if (valueTerm != null)
      si.addOption(RegExFilter.VALUE_REGEX, valueTerm);
    si.addOption(RegExFilter.OR_FIELDS, String.valueOf(orFields));
    si.addOption(RegExFilter.MATCH_SUBSTRING, String.valueOf(matchSubstring));

  }

  /**
   * Set the encoding string to use when interpreting characters
   *
   * @param si
   *          ScanIterator config to be updated
   * @param encoding
   *          the encoding string to use for character interpretation.
   *
   */
  public static void setEncoding(IteratorSetting si, String encoding) {
    if (!encoding.isEmpty()) {
      si.addOption(RegExFilter.ENCODING, encoding);
    }
  }
}
