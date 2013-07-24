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

package org.apache.accumulo.core.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.io.Text;

/**
 * @since 1.6.0
 */
public class ConditionalMutation extends Mutation {
  
  private List<Condition> conditions = new ArrayList<Condition>();

  public ConditionalMutation(byte[] row, Condition condition, Condition... conditions) {
    super(row);
    init(condition, conditions);
  }
  
  public ConditionalMutation(byte[] row, int start, int length, Condition condition, Condition... conditions) {
    super(row, start, length);
    init(condition, conditions);
  }
  
  public ConditionalMutation(Text row, Condition condition, Condition... conditions) {
    super(row);
    init(condition, conditions);
  }
  
  public ConditionalMutation(CharSequence row, Condition condition, Condition... conditions) {
    super(row);
    init(condition, conditions);
  }
  
  public ConditionalMutation(ByteSequence row, Condition condition, Condition... conditions) {
    // TODO add ByteSequence methods to mutations
    super(row.toArray());
    init(condition, conditions);
  }
  
  public ConditionalMutation(ConditionalMutation cm) {
    super(cm);
    this.conditions = new ArrayList<Condition>(cm.conditions);
  }

  private void init(Condition condition, Condition... conditions) {
    ArgumentChecker.notNull(condition);
    this.conditions.add(condition);
    if (conditions.length > 0) {
      this.conditions.addAll(Arrays.asList(conditions));
    }
  }
  
  public void addCondition(Condition condition) {
    ArgumentChecker.notNull(condition);
    this.conditions.add(condition);
  }
  
  public List<Condition> getConditions() {
    return Collections.unmodifiableList(conditions);
  }

}
