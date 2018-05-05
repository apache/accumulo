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
package org.apache.accumulo.master.tableOps.bulkVer2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Bulk;
import org.apache.accumulo.core.client.impl.Bulk.Files;
import org.apache.accumulo.core.client.impl.BulkSerialize;
import org.apache.accumulo.core.client.impl.BulkSerialize.LoadMappingIterator;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.MapFileInfo;
import org.apache.accumulo.core.metadata.schema.MetadataScanner;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Make asynchronous load calls to each overlapping Tablet. This RepO does its work on the isReady
 * and will return a linear sleep value based on the largest number of Tablets on a TabletServer.
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
    VolumeManager fs = master.getFileSystem();
    final Path bulkDir = new Path(bulkInfo.bulkDir);
    try (LoadMappingIterator lmi = BulkSerialize.getUpdatedLoadMapping(bulkDir.toString(),
        bulkInfo.tableId, p -> fs.open(p))) {
      return loadFiles(bulkInfo.tableId, bulkDir, lmi, master, tid);
    }
  }

  @Override
  public Repo<Master> call(final long tid, final Master master) throws Exception {
    return new CompleteBulkImport(bulkInfo.tableId, bulkInfo.sourceDir, bulkInfo.bulkDir);
  }

  static boolean equals(Text t1, Text t2) {
    if (t1 == null || t2 == null)
      return t1 == t2;

    return t1.equals(t2);
  }

  /**
   * Make asynchronous load calls to each overlapping Tablet in the bulk mapping. Return a sleep
   * time to isReady based on a factor of the TabletServer with the most Tablets. This method will
   * scan the metadata table getting Tablet range and location information. It will return 0 when
   * all files have been loaded.
   */
  private long loadFiles(Table.ID tableId, Path bulkDir, LoadMappingIterator lmi, Master master,
      long tid) throws AccumuloSecurityException, TableNotFoundException, AccumuloException {

    Map.Entry<KeyExtent,Bulk.Files> loadMapEntry = lmi.next();

    Text startRow = loadMapEntry.getKey().getPrevEndRow();

    Iterable<TabletMetadata> tableMetadata = MetadataScanner.builder().from(master)
        .overUserTableId(tableId, startRow, null).fetchPrev().fetchLocation().fetchLoaded().build();

    long timeInMillis = master.getConfiguration().getTimeInMillis(Property.MASTER_BULK_TIMEOUT);
    Iterator<TabletMetadata> tabletIter = tableMetadata.iterator();

    List<TabletMetadata> tablets = new ArrayList<>();
    TabletMetadata currentTablet = tabletIter.next();
    HostAndPort server = null;

    // track how many tablets were sent load messages per tablet server
    MapCounter<HostAndPort> tabletsPerServer = new MapCounter<>();

    String fmtTid = String.format("%016x", tid);

    int locationLess = 0;

    long t1 = System.currentTimeMillis();
    while (true) {
      if (loadMapEntry == null) {
        if (!lmi.hasNext()) {
          break;
        }
        loadMapEntry = lmi.next();
      }
      KeyExtent fke = loadMapEntry.getKey();
      Files files = loadMapEntry.getValue();
      loadMapEntry = null;

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

        TabletMetadata.Location location = tablet.getLocation();
        if (location == null) {
          locationLess++;
          continue;
        } else {
          server = location.getHostAndPort();
        }

        Set<String> loadedFiles = tablet.getLoaded();

        Map<String,MapFileInfo> thriftImports = new HashMap<>();

        for (final Bulk.FileInfo fileInfo : files) {
          String fullPath = new Path(bulkDir, fileInfo.getFileName()).toString();

          if (!loadedFiles.contains(fullPath)) {
            thriftImports.put(fileInfo.getFileName(), new MapFileInfo(fileInfo.getEstFileSize()));
          }
        }

        if (thriftImports.size() > 0) {
          // must always increment this even if there is a comms failure, because it indicates there
          // is work to do
          tabletsPerServer.increment(server, 1);
          log.trace("tid {} asking {} to bulk import {} files", fmtTid, server,
              thriftImports.size());
          TabletClientService.Client client = null;
          try {
            client = ThriftUtil.getTServerClient(server, master, timeInMillis);
            client.loadFiles(Tracer.traceInfo(), master.rpcCreds(), tid, fke.toThrift(),
                bulkDir.toString(), thriftImports, bulkInfo.setTime);
          } catch (TException ex) {
            log.debug("rpc failed server: " + server + ", tid:" + fmtTid + " " + ex.getMessage(),
                ex);
          } finally {
            ThriftUtil.returnClient(client);
          }
        }
      }
    }
    long t2 = System.currentTimeMillis();

    long sleepTime = 0;
    if (tabletsPerServer.size() > 0) {
      // find which tablet server had the most load messages sent to it and sleep 13ms for each load
      // message
      sleepTime = Collections.max(tabletsPerServer.values()) * 13;
    }

    if (locationLess > 0) {
      sleepTime = Math.max(100, Math.max(2 * (t2 - t1), sleepTime));
    }

    return sleepTime;
  }
}
