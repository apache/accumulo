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

package org.apache.accumulo.core.constraints;

import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;

/**
 * This constraint ensures that all deletes in a mutation must specify a timestamp.
 * 
 * <p>
 * This constraint is useful in conjunction with exact deletes.
 *
 * @see NewTableConfiguration#setExactDeleteEnabled(boolean)
 * @since 2.0.0
 */
public class NoTimestampDeleteConstraint implements Constraint {

  @Override
  public String getViolationDescription(short violationCode) {
    if (violationCode == 1) {
      return "Delete did not specify a timestamp";
    }
    return null;
  }

  @Override
  public List<Short> check(Environment env, Mutation mutation) {
    List<ColumnUpdate> updates = mutation.getUpdates();
    for (ColumnUpdate update : updates) {
      if (!update.hasTimestamp()) {
        return Collections.singletonList((short) 1);
      }
    }

    return null;
  }

}
