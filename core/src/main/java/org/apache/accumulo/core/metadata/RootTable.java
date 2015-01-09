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
package org.apache.accumulo.core.metadata;

import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class RootTable {

  public static final String ID = "+r";
  public static final String NAME = Namespaces.ACCUMULO_NAMESPACE + ".root";

  /**
   * DFS location relative to the Accumulo directory
   */
  public static final String ROOT_TABLET_LOCATION = "/root_tablet";

  /**
   * ZK path relative to the instance directory for information about the root tablet
   */
  public static final String ZROOT_TABLET = ROOT_TABLET_LOCATION;
  public static final String ZROOT_TABLET_LOCATION = ZROOT_TABLET + "/location";
  public static final String ZROOT_TABLET_FUTURE_LOCATION = ZROOT_TABLET + "/future_location";
  public static final String ZROOT_TABLET_LAST_LOCATION = ZROOT_TABLET + "/lastlocation";
  public static final String ZROOT_TABLET_WALOGS = ZROOT_TABLET + "/walogs";
  public static final String ZROOT_TABLET_PATH = ZROOT_TABLET + "/dir";

  public static final KeyExtent EXTENT = new KeyExtent(new Text(ID), null, null);
  public static final KeyExtent OLD_EXTENT = new KeyExtent(new Text(MetadataTable.ID), KeyExtent.getMetadataEntry(new Text(MetadataTable.ID), null), null);

}
