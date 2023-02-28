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

package org.apache.accumulo.server.metadata;

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.metadata.iterators.AnyLocationIterator;
import org.apache.accumulo.server.metadata.iterators.PresentIterator;
import org.apache.hadoop.io.Text;

public class ConditionalTabletMutatorImpl extends TabletMutatorBase<Ample.ConditionalTabletMutator>
    implements Ample.ConditionalTabletMutator {

  private static final int INITIAL_ITERATOR_PRIO = 1000000;

  private final ConditionalMutation mutation;

  protected ConditionalTabletMutatorImpl(ServerContext context, KeyExtent extent) {
    super(context, extent, new ConditionalMutation(extent.toMetaRow()));
    this.mutation = (ConditionalMutation) super.mutation;
  }

  @Override
  public Ample.ConditionalTabletMutator requireAbsentLocation() {
    // TODO check if updates are still allowed like super class does
    IteratorSetting is = new IteratorSetting(INITIAL_ITERATOR_PRIO, AnyLocationIterator.class);
    Condition c = new Condition("", "").setIterators(is);
    mutation.addCondition(c);
    return this;
  }

  @Override
  public Ample.ConditionalTabletMutator requireLocation(TServerInstance tsi,
      TabletMetadata.LocationType type) {
    Condition c =
        new Condition(getLocationFamily(type), tsi.getSession()).setValue(tsi.getHostPort());
    mutation.addCondition(c);
    return this;
  }

  @Override
  public Ample.ConditionalTabletMutator requireFile(StoredTabletFile path) {
    IteratorSetting is = new IteratorSetting(INITIAL_ITERATOR_PRIO, PresentIterator.class);
    Condition c = new Condition(DataFileColumnFamily.NAME, path.getMetaUpdateDeleteText())
        .setValue(PresentIterator.VALUE).setIterators(is);
    mutation.addCondition(c);
    return this;
  }

  @Override
  public Ample.ConditionalTabletMutator requirePrevEndRow(Text per) {
    Condition c =
        new Condition(PREV_ROW_COLUMN.getColumnFamily(), PREV_ROW_COLUMN.getColumnQualifier())
            .setValue(per);
    mutation.addCondition(c);
    return this;
  }

  @Override
  public void queue() {

  }
}
