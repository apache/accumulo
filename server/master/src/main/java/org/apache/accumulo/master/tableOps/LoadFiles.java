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
package org.apache.accumulo.master.tableOps;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Bulk;
import org.apache.accumulo.core.client.impl.BulkImport;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.metadata.schema.MetadataScanner;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.util.BulkSerialize;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Make asynchronous load calls to each overlapping Tablet. This RepO does its work on the isReady
 * and will return a linear sleep value based on the number of load calls remaining.
 */
class LoadFiles extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(LoadFiles.class);

  private final BulkInfo bulkInfo;

  public LoadFiles(BulkInfo bulkInfo) {
    this.bulkInfo = bulkInfo;
  }

  @Override
  public long isReady(long tid, Master master) throws Exception {
    if (master.onlineTabletServers().size() == 0) {
      log.warn("There are no tablet server to process bulkDir import, waiting (tid = " + tid + ")");
      return 100;
    }
    master.updateBulkImportStatus(bulkInfo.sourceDir, BulkImportState.LOADING);
    VolumeManager fs = master.getFileSystem();
    final Path bulkDir = new Path(bulkInfo.bulkDir);
    SortedMap<KeyExtent,Bulk.Files> loadMapping = BulkSerialize
        .getUpdatedLoadMapping(bulkDir.toString(), bulkInfo.tableId, fs);

    log.debug("tid " + tid + " sending files for " + loadMapping.firstKey() + " to "
        + loadMapping.lastKey());
    int fileLoadsRemaining = sendFiles(bulkInfo.tableId, bulkDir, loadMapping, master, tid);
    if (fileLoadsRemaining > 0) {
      int sleep = fileLoadsRemaining * 100;
      log.debug("Sleep for {} - tid {} has {} file load{} remaining.", sleep, tid,
          fileLoadsRemaining, fileLoadsRemaining > 1 ? "s" : "");
      return sleep;
    }

    return 0;
  }

  @Override
  public Repo<Master> call(final long tid, final Master master) throws Exception {
    // return the next step, which will verify
    return new CompleteBulkImport(bulkInfo.tableId, bulkInfo.sourceDir, bulkInfo.bulkDir, null);
  }

  private boolean equals(Text t1, Text t2) {
    if (t1 == null || t2 == null)
      return t1 == t2;

    return t1.equals(t2);
  }

  /**
   * Make asynchronous load calls to each overlapping Tablet in the bulk mapping. Return the number
   * of load calls remaining. This method will scan the metadata table getting Tablet range and
   * location information. It will also check if the file has been loaded in that Tablet.
   */
  private int sendFiles(Table.ID tableId, Path bulkDir, SortedMap<KeyExtent,Bulk.Files> mapping,
      Master master, long tid)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    int fileLoadsRemaining = 0;
    // TODO restrict range of scan based on min and max rows in key extents.
    Iterable<TabletMetadata> tableMetadata = MetadataScanner.builder().from(master)
        .overUserTableId(tableId).fetchPrev().fetchLocation().fetchLoaded().build();
    long timeInMillis = master.getConfiguration().getTimeInMillis(Property.MASTER_BULK_TIMEOUT);
    Iterator<TabletMetadata> tabletIter = tableMetadata.iterator();
    Iterator<Map.Entry<KeyExtent,Bulk.Files>> loadMappingIter = mapping.entrySet().iterator();

    List<TabletMetadata> tablets = new ArrayList<>();
    TabletMetadata currentTablet = tabletIter.next();
    HostAndPort server = null;

    while (loadMappingIter.hasNext()) {
      Map.Entry<KeyExtent,Bulk.Files> loadMapEntry = loadMappingIter.next();
      KeyExtent fke = loadMapEntry.getKey();
      tablets.clear();

      while (!equals(currentTablet.getPrevEndRow(), fke.getPrevEndRow())) {
        currentTablet = tabletIter.next();
      }
      tablets.add(currentTablet);

      while (!equals(currentTablet.getEndRow(), fke.getEndRow())) {
        currentTablet = tabletIter.next();
        tablets.add(currentTablet);
      }

      for (TabletMetadata tablet : tablets) {
        // send files to tablet sever
        // ideally there should only be one tablet location to send all the files
        try {
          TabletMetadata.Location location = tablet.getLocation();
          if (location == null) {
            fileLoadsRemaining += loadMapEntry.getValue().getSize();
            continue;
          } else {
            server = location.getHostAndPort();
          }

          TabletClientService.Iface client = ThriftUtil.getTServerClient(server, master,
              timeInMillis);
          Set<String> loadedFiles = tablet.getLoaded();
          for (final Bulk.FileInfo fileInfo : loadMapEntry.getValue()) {
            String fullPath = new Path(bulkDir, fileInfo.getFileName()).toString();
            if (!loadedFiles.contains(fullPath)) {
              // keep sending loads until file is loaded
              fileLoadsRemaining++;
              log.debug("Asking " + server + " to bulk import " + fullPath);
              client.loadFile(Tracer.traceInfo(), master.rpcCreds(), tid, fullPath,
                  fileInfo.getEstFileSize(), BulkImport.wrapKeyExtent(fke), bulkInfo.setTime);
            }
          }
        } catch (Exception ex) {
          log.error("rpc failed server: " + server + ", tid:" + tid + " " + ex.getMessage(), ex);
        }
      }
    }
    return fileLoadsRemaining;
  }

}
