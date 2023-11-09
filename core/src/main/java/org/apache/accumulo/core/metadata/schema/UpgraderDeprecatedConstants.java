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

import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.hadoop.io.Text;

/**
 * MetadataSchema constants that are deprecated and should only be used to support removals during
 * the upgrade process.
 */
public class UpgraderDeprecatedConstants {

  /**
   * ChoppedColumnFamily kept around for cleaning up old entries on upgrade. Currently not used,
   * will be used by #3768
   */
  public static class ChoppedColumnFamily {
    public static final String STR_NAME = "chopped";
    public static final Text NAME = new Text(STR_NAME);
    public static final ColumnFQ CHOPPED_COLUMN = new ColumnFQ(NAME, new Text(STR_NAME));
  }

}
