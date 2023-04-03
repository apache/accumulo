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
package org.apache.accumulo.server.util;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.Ample.TabletMutator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.server.ServerContext;

public class ManagerMetadataUtil {

  public static void addNewTablet(ServerContext context, KeyExtent extent, String dirName,
      TServerInstance tServerInstance, Map<StoredTabletFile,DataFileValue> datafileSizes,
      Map<Long,? extends Collection<TabletFile>> bulkLoadedFiles, MetadataTime time,
      long lastFlushID, long lastCompactID, ServiceLock zooLock) {

    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    tablet.putPrevEndRow(extent.prevEndRow());
    tablet.putZooLock(zooLock);
    tablet.putDirName(dirName);
    tablet.putTime(time);

    if (lastFlushID > 0) {
      tablet.putFlushId(lastFlushID);
    }

    if (lastCompactID > 0) {
      tablet.putCompactionId(lastCompactID);
    }

    if (tServerInstance != null) {
      tablet.putLocation(Location.current(tServerInstance));
      tablet.deleteLocation(Location.future(tServerInstance));
    }

    datafileSizes.forEach(tablet::putFile);

    for (Entry<Long,? extends Collection<TabletFile>> entry : bulkLoadedFiles.entrySet()) {
      for (TabletFile ref : entry.getValue()) {
        tablet.putBulkFile(ref, entry.getKey());
      }
    }

    tablet.mutate();
  }
}
