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
package org.apache.accumulo.server.gc;

import static org.apache.accumulo.server.gc.GcVolumeUtil.ALL_VOLUMES_PREFIX;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.hadoop.fs.Path;

/**
 * A specially encoded GC Reference to a directory with the {@link GcVolumeUtil#ALL_VOLUMES_PREFIX}
 */
public class AllVolumesDirectory extends ReferenceFile {

  public AllVolumesDirectory(TableId tableId, String dirName) {
    super(tableId, getDeleteTabletOnAllVolumesUri(tableId, dirName));
  }

  private static String getDeleteTabletOnAllVolumesUri(TableId tableId, String dirName) {
    MetadataSchema.TabletsSection.ServerColumnFamily.validateDirCol(dirName);
    return ALL_VOLUMES_PREFIX + Constants.TABLE_DIR + Path.SEPARATOR + tableId + Path.SEPARATOR
        + dirName;
  }

  @Override
  public String getMetadataEntry() {
    return metadataEntry;
  }

  @Override
  public boolean isDirectory() {
    return true;
  }
}
