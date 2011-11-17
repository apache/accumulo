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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Caller sets up this filter for the rows that they want to match on and then this filter ignores those rows during scans and compactions.
 * 
 */
/**
 * @deprecated since 1.4
 * @use org.apache.accumulo.core.iterators.Filter with negate flag set to <tt>true</tt>
 **/
public class DeleteFilter extends RegExFilter {
  
  @Override
  public boolean accept(Key key, Value value) {
    return !(super.accept(key, value));
  }
  
}
