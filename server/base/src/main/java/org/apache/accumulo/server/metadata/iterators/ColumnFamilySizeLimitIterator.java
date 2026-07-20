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
package org.apache.accumulo.server.metadata.iterators;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.server.metadata.ConditionalTabletMutatorImpl;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * Iterator that checks if a column family size is less than or equal a limit as part of a
 * conditional mutation.
 */
public class ColumnFamilySizeLimitIterator extends ColumnFamilyTransformationIterator {

  private static final String LIMIT_OPT = "limit";

  private Long limit;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    limit = Long.parseLong(options.get(LIMIT_OPT));
    Preconditions.checkState(limit >= 0);
  }

  @Override
  protected Value transform(SortedKeyValueIterator<Key,Value> source) throws IOException {
    long count = 0;
    while (source.hasTop()) {
      source.next();
      count++;
    }

    if (count <= limit) {
      return new Value("1");
    } else {
      return null;
    }
  }

  /**
   * Create a condition that checks if the specified column family's size is less than or equal to
   * the given limit.
   */
  public static Condition createCondition(Text family, long limit) {
    IteratorSetting is = new IteratorSetting(ConditionalTabletMutatorImpl.INITIAL_ITERATOR_PRIO,
        ColumnFamilySizeLimitIterator.class);
    is.addOption(LIMIT_OPT, limit + "");
    return new Condition(family, EMPTY).setValue("1").setIterators(is);
  }
}
