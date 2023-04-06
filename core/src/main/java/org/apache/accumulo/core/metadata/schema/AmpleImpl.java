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
package org.apache.accumulo.core.metadata.schema;

import static com.google.common.collect.MoreCollectors.onlyElement;

import java.util.NoSuchElementException;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata.Options;

public class AmpleImpl implements Ample {
  private final AccumuloClient client;

  public AmpleImpl(AccumuloClient client) {
    this.client = client;
  }

  @Override
  public TabletMetadata readTablet(KeyExtent extent, ReadConsistency readConsistency,
      ColumnType... colsToFetch) {
    Options builder = TabletsMetadata.builder(client).forTablet(extent);
    if (colsToFetch.length > 0) {
      builder.fetch(colsToFetch);
    }

    builder.readConsistency(readConsistency);

    try (TabletsMetadata tablets = builder.build()) {
      return tablets.stream().collect(onlyElement());
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public TabletsMetadata.TableOptions readTablets() {
    return TabletsMetadata.builder(this.client);
  }

}
