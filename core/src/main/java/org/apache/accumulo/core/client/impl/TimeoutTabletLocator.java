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
package org.apache.accumulo.core.client.impl;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.hadoop.io.Text;

/**
 * Throws a {@link TimedOutException} if the specified timeout duration elapses between two failed TabletLocator calls.
 * <p>
 * This class is safe to cache locally.
 */
public class TimeoutTabletLocator extends SyncingTabletLocator {

  private long timeout;
  private Long firstFailTime = null;

  private void failed() {
    if (firstFailTime == null) {
      firstFailTime = System.currentTimeMillis();
    } else if (System.currentTimeMillis() - firstFailTime > timeout) {
      throw new TimedOutException("Failed to obtain metadata");
    }
  }

  private void succeeded() {
    firstFailTime = null;
  }

  public TimeoutTabletLocator(long timeout, final ClientContext context, final Table.ID table) {
    super(context, table);
    this.timeout = timeout;
  }

  @Override
  public TabletLocation locateTablet(ClientContext context, Text row, boolean skipRow, boolean retry) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    try {
      TabletLocation ret = super.locateTablet(context, row, skipRow, retry);

      if (ret == null)
        failed();
      else
        succeeded();

      return ret;
    } catch (AccumuloException ae) {
      failed();
      throw ae;
    }
  }

  @Override
  public <T extends Mutation> void binMutations(ClientContext context, List<T> mutations, Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    try {
      super.binMutations(context, mutations, binnedMutations, failures);

      if (failures.size() == mutations.size())
        failed();
      else
        succeeded();

    } catch (AccumuloException ae) {
      failed();
      throw ae;
    }
  }

  @Override
  public List<Range> binRanges(ClientContext context, List<Range> ranges, Map<String,Map<KeyExtent,List<Range>>> binnedRanges) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException {
    try {
      List<Range> ret = super.binRanges(context, ranges, binnedRanges);

      if (ranges.size() == ret.size())
        failed();
      else
        succeeded();

      return ret;
    } catch (AccumuloException ae) {
      failed();
      throw ae;
    }
  }
}
