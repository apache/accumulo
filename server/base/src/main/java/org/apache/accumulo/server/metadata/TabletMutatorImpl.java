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

import java.util.Objects;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMutatorBase;
import org.apache.accumulo.server.ServerContext;

class TabletMutatorImpl extends TabletMutatorBase<Ample.TabletMutator>
    implements Ample.TabletMutator {

  private final ServerContext context;
  private final ServiceLock lock;
  private final BatchWriter writer;

  TabletMutatorImpl(ServerContext context, KeyExtent extent, BatchWriter batchWriter) {
    super(extent);
    this.context = context;
    this.lock = this.context.getServiceLock();
    this.writer = batchWriter;
    Objects.requireNonNull(this.lock, "ServiceLock not set on ServerContext");
  }

  @Override
  public void mutate() {
    try {
      this.putZooLock(this.context.getZooKeeperRoot(), lock);
      writer.addMutation(getMutation());

      if (closeAfterMutate != null) {
        closeAfterMutate.close();
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
