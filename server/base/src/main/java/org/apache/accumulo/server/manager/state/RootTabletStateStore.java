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
package org.apache.accumulo.server.manager.state;

import java.util.List;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;

import com.google.common.base.Preconditions;

class RootTabletStateStore extends MetaDataStateStore {

  RootTabletStateStore(DataLevel level, ClientContext context) {
    super(level, context, AccumuloTable.ROOT.tableName());
  }

  @Override
  public ClosableIterator<TabletManagement> iterator(List<Range> ranges,
      TabletManagementParameters parameters) {
    Preconditions.checkArgument(parameters.getLevel() == getLevel());
    return new TabletManagementScanner(context, ranges, parameters, AccumuloTable.ROOT.tableName());
  }

  @Override
  public String name() {
    return "Metadata Tablets";
  }
}
