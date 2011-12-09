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
import java.io.UnsupportedEncodingException;
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
    RegExFilter result = new RegExFilter();
    result.setSource(getSource().deepCopy(env));
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
  
  public static final String ENCODING_DEFAULT = "UTF-8";
  
  private Matcher rowMatcher;
  private Matcher colfMatcher;
  private Matcher colqMatcher;
  private Matcher valueMatcher;
  private boolean orFields = false;
  
  private String encoding = ENCODING_DEFAULT;
  
  private Matcher copyMatcher(Matcher m) {
    if (m == null)
      return m;
    else
      return m.pattern().matcher("");
  }
  
  private boolean matches(Matcher matcher, ByteSequence bs) {
    if (matcher != null) {
      try {
        matcher.reset(new String(bs.getBackingArray(), bs.offset(), bs.length(), encoding));
        return matcher.matches();
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
    }
    return !orFields;
  }
  
  private boolean matches(Matcher matcher, byte data[], int offset, int len) {
    if (matcher != null) {
      try {
        matcher.reset(new String(data, offset, len, encoding));
        return matcher.matches();
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
    }
    return !orFields;
  }
  
  @Override
  public boolean accept(Key key, Value value) {
    if (orFields)
      return matches(rowMatcher, key.getRowData()) || matches(colfMatcher, key.getColumnFamilyData()) || matches(colqMatcher, key.getColumnQualifierData())
          || matches(valueMatcher, value.get(), 0, value.get().length);
    return matches(rowMatcher, key.getRowData()) && matches(colfMatcher, key.getColumnFamilyData()) && matches(colqMatcher, key.getColumnQualifierData())
        && matches(valueMatcher, value.get(), 0, value.get().length);
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
    
    if (options.containsKey(ENCODING)) {
      encoding = options.get(ENCODING);
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
    io.addNamedOption(RegExFilter.OR_FIELDS, "use OR instread of AND when multiple regexes given");
    io.addNamedOption(RegExFilter.ENCODING, "character encoding of byte array value (default is " + ENCODING_DEFAULT + ")");
    return io;
  }
  
  @Override
  public boolean validateOptions(Map<String,String> options) {
    super.validateOptions(options);
    if (options.containsKey(ROW_REGEX))
      Pattern.compile(options.get(ROW_REGEX)).matcher("");
    
    if (options.containsKey(COLF_REGEX))
      Pattern.compile(options.get(COLF_REGEX)).matcher("");
    
    if (options.containsKey(COLQ_REGEX))
      Pattern.compile(options.get(COLQ_REGEX)).matcher("");
    
    if (options.containsKey(VALUE_REGEX))
      Pattern.compile(options.get(VALUE_REGEX)).matcher("");
    
    if (options.containsKey(ENCODING)) {
      try {
        this.encoding = options.get(ENCODING);
        @SuppressWarnings("unused")
        String test = new String("test".getBytes(), encoding);
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
        return false;
      }
    }
    
    return true;
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
   * @param orFields
   *          if true, any of the non-null terms can match to return the entry
   */
  public static void setRegexs(IteratorSetting si, String rowTerm, String cfTerm, String cqTerm, String valueTerm, boolean orFields) {
    if (rowTerm != null)
      si.addOption(RegExFilter.ROW_REGEX, rowTerm);
    if (cfTerm != null)
      si.addOption(RegExFilter.COLF_REGEX, cfTerm);
    if (cqTerm != null)
      si.addOption(RegExFilter.COLQ_REGEX, cqTerm);
    if (valueTerm != null)
      si.addOption(RegExFilter.VALUE_REGEX, valueTerm);
    if (orFields) {
      si.addOption(RegExFilter.OR_FIELDS, "true");
    }
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
