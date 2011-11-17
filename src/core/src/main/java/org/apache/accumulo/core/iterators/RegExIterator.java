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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.filter.RegExFilter;

/**
 * @deprecated since 1.4
 * @use org.apache.accumulo.core.iterators.user.RegExFilter
 **/
public class RegExIterator extends SkippingIterator implements OptionDescriber {
  
  private RegExFilter ref = new RegExFilter();
  
  public RegExIterator deepCopy(IteratorEnvironment env) {
    return new RegExIterator(this, env);
  }
  
  private RegExIterator(RegExIterator other, IteratorEnvironment env) {
    setSource(other.getSource().deepCopy(env));
    ref = other.ref;
  }
  
  public RegExIterator() {
    
  }
  
  private boolean matches(Key key, Value value) {
    return ref.accept(key, value);
  }
  
  @Override
  protected void consume() throws IOException {
    while (getSource().hasTop() && !matches(getSource().getTopKey(), getSource().getTopValue())) {
      getSource().next();
    }
  }
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    ref.init(options);
  }
  
  @Override
  public IteratorOptions describeOptions() {
    return ref.describeOptions();
  }
  
  @Override
  public boolean validateOptions(Map<String,String> options) {
    return ref.validateOptions(options);
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
}
