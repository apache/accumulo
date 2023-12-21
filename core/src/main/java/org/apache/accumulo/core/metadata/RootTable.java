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
package org.apache.accumulo.core.metadata;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;

public class RootTable {

  public static final TableId ID = TableId.of("+r");

  public static final String NAME = Namespace.ACCUMULO.name() + ".root";

  /**
   * DFS location relative to the Accumulo directory
   */
  public static final String ROOT_TABLET_DIR_NAME = "root_tablet";

  /**
   * ZK path relative to the zookeeper node where the root tablet metadata is stored.
   */
  public static final String ZROOT_TABLET = "/" + ROOT_TABLET_DIR_NAME;

  /**
   * ZK path relative to the zookeeper node where the root tablet gc candidates are stored.
   */
  public static final String ZROOT_TABLET_GC_CANDIDATES = ZROOT_TABLET + "/gc_candidates";

  public static final KeyExtent EXTENT = new KeyExtent(ID, null, null);
  public static final KeyExtent OLD_EXTENT =
      new KeyExtent(MetadataTable.ID, TabletsSection.encodeRow(MetadataTable.ID, null), null);

}
