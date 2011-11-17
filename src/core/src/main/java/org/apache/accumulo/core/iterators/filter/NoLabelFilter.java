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
package org.apache.accumulo.core.iterators.filter;

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * @deprecated since 1.4
 * @use org.apache.accumulo.core.iterators.user.NoVisFilter
 **/
public class NoLabelFilter implements Filter, OptionDescriber {
  
  @Override
  public boolean accept(Key k, Value v) {
    ColumnVisibility label = new ColumnVisibility(k.getColumnVisibility());
    return label.getExpression().length > 0;
    
  }
  
  @Override
  public void init(Map<String,String> options) {
    // No Options to set
  }
  
  @Override
  public IteratorOptions describeOptions() {
    return new IteratorOptions("nolabel", "NoLabelFilter hides entries without a visibility label", null, null);
  }
  
  @Override
  public boolean validateOptions(Map<String,String> options) {
    return true;
  }
}
