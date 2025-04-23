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

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.server.metadata.ConditionalTabletMutatorImpl;
import org.apache.hadoop.io.Text;

public class CurrentLocationNotEqualToIterator extends ColumnFamilyTransformationIterator {
  private static final String TSERVER_INSTANCE_OPTION = "tsi_option";
  private static final String NOT_EQUAL = "0";
  private static final String EQUAL = "1";
  private TServerInstance tsi;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    tsi = new TServerInstance(options.get(TSERVER_INSTANCE_OPTION));
  }

  @Override
  protected Value transform(SortedKeyValueIterator<Key,Value> source) throws IOException {
    TServerInstance tsiSeen = null;
    while (source.hasTop()) {
      Value address = source.getTopValue();
      Text session = source.getTopKey().getColumnQualifier();
      tsiSeen = new TServerInstance(address, session);
      if (tsiSeen.equals(tsi)) {
        break;
      }
      source.next();
    }

    if (tsi.equals(tsiSeen)) {
      return new Value(EQUAL);
    } else {
      return new Value(NOT_EQUAL);
    }
  }

  /**
   * Create a condition that fails if the {@link CurrentLocationColumnFamily} has an entry which is
   * equal to the given {@link TServerInstance}, passing otherwise
   */
  public static Condition createCondition(TServerInstance tsi) {
    IteratorSetting is = new IteratorSetting(ConditionalTabletMutatorImpl.INITIAL_ITERATOR_PRIO,
        CurrentLocationNotEqualToIterator.class);
    is.addOption(TSERVER_INSTANCE_OPTION, tsi.getHostPortSession());
    return new Condition(CurrentLocationColumnFamily.NAME, EMPTY).setValue(NOT_EQUAL)
        .setIterators(is);
  }
}
