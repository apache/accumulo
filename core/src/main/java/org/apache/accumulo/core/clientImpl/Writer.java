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
package org.apache.accumulo.core.clientImpl;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.Ample;

public class Writer {

  private ClientContext context;
  private TableId tableId;

  public Writer(ClientContext context, TableId tableId) {
    checkArgument(context != null, "context is null");
    checkArgument(tableId != null, "tableId is null");
    this.context = context;
    this.tableId = tableId;
  }

  public void update(Mutation m) throws AccumuloException, TableNotFoundException {
    checkArgument(m != null, "m is null");

    if (m.size() == 0) {
      throw new IllegalArgumentException("Can not add empty mutations");
    }

    String table = Ample.DataLevel.of(tableId).metaTable();

    try (var writer = context.createBatchWriter(table)) {
      writer.addMutation(m);
    }

  }
}
