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
package org.apache.accumulo.tserver.tablet;

import java.util.List;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.accumulo.tserver.log.DfsLogger;

/*
 * A partial interface of Tablet to allow for testing of CommitSession without needing a real Tablet.
 */
public interface TabletCommitter {

  void abortCommit(CommitSession commitSession, List<Mutation> value);

  void commit(CommitSession commitSession, List<Mutation> mutations);

  /**
   * If this method returns true, the caller must call {@link #finishUpdatingLogsUsed()} to clean up
   */
  boolean beginUpdatingLogsUsed(InMemoryMap memTable, DfsLogger copy, boolean mincFinish);

  void finishUpdatingLogsUsed();

  TableConfiguration getTableConfiguration();

  /**
   * Returns a KeyExtent object representing this tablet's key range.
   *
   * @return extent
   */
  KeyExtent getExtent();

  int getLogId();

  Durability getDurability();

  void updateMemoryUsageStats(long estimatedSizeInBytes, long estimatedSizeInBytes2);

}
