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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.clientImpl.Bulk;
import org.apache.accumulo.core.clientImpl.Bulk.Files;
import org.apache.accumulo.core.clientImpl.BulkSerialize;
import org.apache.accumulo.core.clientImpl.BulkSerialize.LoadMappingIterator;
import org.apache.accumulo.core.clientImpl.Table;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.MapFileInfo;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

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
    if (bulkInfo.tableState == TableState.ONLINE) {
      return new CompleteBulkImport(bulkInfo);
    } else {
      return new CleanUpBulkImport(bulkInfo);
    }
  }

  private abstract static class Loader {
    protected Path bulkDir;
    protected Master master;
    protected long tid;
    protected boolean setTime;

    void start(Path bulkDir, Master master, long tid, boolean setTime) throws Exception {
      this.bulkDir = bulkDir;
      this.master = master;
      this.tid = tid;
      this.setTime = setTime;
    }

    abstract void load(List<TabletMetadata> tablets, Files files) throws Exception;

    abstract long finish() throws Exception;
  }

  private static class OnlineLoader extends Loader {

    long timeInMillis;
    String fmtTid;
    int locationLess = 0;

    // track how many tablets were sent load messages per tablet server
    MapCounter<HostAndPort> loadMsgs;

    @Override
    void start(Path bulkDir, Master master, long tid, boolean setTime) throws Exception {
      super.start(bulkDir, master, tid, setTime);

      timeInMillis = master.getConfiguration().getTimeInMillis(Property.MASTER_BULK_TIMEOUT);
      fmtTid = String.format("%016x", tid);

      loadMsgs = new MapCounter<>();
    }

    @Override
    void load(List<TabletMetadata> tablets, Files files) {
      for (TabletMetadata tablet : tablets) {
        // send files to tablet sever
        // ideally there should only be one tablet location to send all the files

        TabletMetadata.Location location = tablet.getLocation();
        HostAndPort server = null;
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
          loadMsgs.increment(server, 1);
          log.trace("tid {} asking {} to bulk import {} files", fmtTid, server,
              thriftImports.size());
          TabletClientService.Client client = null;
          try {
            client = ThriftUtil.getTServerClient(server, master.getContext(), timeInMillis);
            client.loadFiles(Tracer.traceInfo(), master.getContext().rpcCreds(), tid,
                tablet.getExtent().toThrift(), bulkDir.toString(), thriftImports, setTime);
          } catch (TException ex) {
            log.debug("rpc failed server: " + server + ", tid:" + fmtTid + " " + ex.getMessage(),
                ex);
          } finally {
            ThriftUtil.returnClient(client);
          }
        }
      }

    }

    @Override
    long finish() {
      long sleepTime = 0;
      if (loadMsgs.size() > 0) {
        // find which tablet server had the most load messages sent to it and sleep 13ms for each
        // load message
        sleepTime = Collections.max(loadMsgs.values()) * 13;
      }

      if (locationLess > 0) {
        sleepTime = Math.max(Math.max(100L, locationLess), sleepTime);
      }

      return sleepTime;
    }

  }

  private static class OfflineLoader extends Loader {

    BatchWriter bw;

    // track how many tablets were sent load messages per tablet server
    MapCounter<HostAndPort> unloadingTablets;

    @Override
    void start(Path bulkDir, Master master, long tid, boolean setTime) throws Exception {
      Preconditions.checkArgument(!setTime);
      super.start(bulkDir, master, tid, setTime);
      bw = master.getClient().createBatchWriter(MetadataTable.NAME);
      unloadingTablets = new MapCounter<>();
    }

    @Override
    void load(List<TabletMetadata> tablets, Files files) throws MutationsRejectedException {
      byte[] fam = TextUtil.getBytes(DataFileColumnFamily.NAME);

      for (TabletMetadata tablet : tablets) {
        if (tablet.getLocation() != null) {
          unloadingTablets.increment(tablet.getLocation().getHostAndPort(), 1L);
          continue;
        }

        Mutation mutation = new Mutation(tablet.getExtent().getMetadataEntry());

        for (final Bulk.FileInfo fileInfo : files) {
          String fullPath = new Path(bulkDir, fileInfo.getFileName()).toString();
          byte[] val = new DataFileValue(fileInfo.getEstFileSize(), fileInfo.getEstNumEntries())
              .encode();
          mutation.put(fam, fullPath.getBytes(UTF_8), val);
        }

        bw.addMutation(mutation);
      }
    }

    @Override
    long finish() throws Exception {

      bw.close();

      long sleepTime = 0;
      if (unloadingTablets.size() > 0) {
        // find which tablet server had the most tablets to unload and sleep 13ms for each tablet
        sleepTime = Collections.max(unloadingTablets.values()) * 13;
      }

      return sleepTime;
    }
  }

  /**
   * Make asynchronous load calls to each overlapping Tablet in the bulk mapping. Return a sleep
   * time to isReady based on a factor of the TabletServer with the most Tablets. This method will
   * scan the metadata table getting Tablet range and location information. It will return 0 when
   * all files have been loaded.
   */
  private long loadFiles(Table.ID tableId, Path bulkDir, LoadMappingIterator lmi, Master master,
      long tid) throws Exception {

    Map.Entry<KeyExtent,Bulk.Files> loadMapEntry = lmi.next();

    Text startRow = loadMapEntry.getKey().getPrevEndRow();

    Iterator<TabletMetadata> tabletIter = TabletsMetadata.builder().forTable(tableId)
        .overlapping(startRow, null).checkConsistency().fetchPrev().fetchLocation().fetchLoaded()
        .build(master.getContext()).iterator();

    List<TabletMetadata> tablets = new ArrayList<>();
    TabletMetadata currentTablet = tabletIter.next();

    Loader loader;
    if (bulkInfo.tableState == TableState.ONLINE) {
      loader = new OnlineLoader();
    } else {
      loader = new OfflineLoader();
    }

    loader.start(bulkDir, master, tid, bulkInfo.setTime);

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

      while (!Objects.equals(currentTablet.getPrevEndRow(), fke.getPrevEndRow())) {
        currentTablet = tabletIter.next();
      }
      tablets.add(currentTablet);

      while (!Objects.equals(currentTablet.getEndRow(), fke.getEndRow())) {
        currentTablet = tabletIter.next();
        tablets.add(currentTablet);
      }

      loader.load(tablets, files);
    }
    long t2 = System.currentTimeMillis();

    long sleepTime = loader.finish();
    if (sleepTime > 0) {
      long scanTime = Math.min(t2 - t1, 30000);
      sleepTime = Math.max(sleepTime, scanTime * 2);
    }
    return sleepTime;
  }
}
