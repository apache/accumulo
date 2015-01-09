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
package org.apache.accumulo.server.master.state;

import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.server.AccumuloServerContext;

public class RootTabletStateStore extends MetaDataStateStore {

  public RootTabletStateStore(ClientContext context, CurrentState state) {
    super(context, state, RootTable.NAME);
  }

  public RootTabletStateStore(AccumuloServerContext context) {
    super(context, RootTable.NAME);
  }

  @Override
  public ClosableIterator<TabletLocationState> iterator() {
    return new MetaDataTableScanner(context, MetadataSchema.TabletsSection.getRange(), state, RootTable.NAME);
  }

  @Override
  public String name() {
    return "Metadata Tablets";
  }
}
