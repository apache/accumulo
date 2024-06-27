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
package org.apache.accumulo.server.init;

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemInitializer {
  private static final String TABLE_TABLETS_TABLET_DIR = "table_info";
  private static final Logger log = LoggerFactory.getLogger(FileSystemInitializer.class);
  private static final Text SPLIT_POINT =
      MetadataSchema.TabletsSection.getRange().getEndKey().getRow();

  // config only for root table
  private final InitialConfiguration initConfig;

  public FileSystemInitializer(InitialConfiguration initConfig) {
    this.initConfig = initConfig;
  }

  private static class Tablet {
    TableId tableId;
    String dirName;
    Text prevEndRow, endRow;
    String[] files;

    Tablet(TableId tableId, String dirName, Text prevEndRow, Text endRow, String... files) {
      this.tableId = tableId;
      this.dirName = dirName;
      this.prevEndRow = prevEndRow;
      this.endRow = endRow;
      this.files = files;
    }
  }

  void initialize(VolumeManager fs, String rootTabletDirUri, String rootTabletFileUri,
      ServerContext context) throws IOException, InterruptedException, KeeperException {
    // initialize initial system tables config in zookeeper
    initSystemTablesConfig(context);

    VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironmentImpl(
        VolumeChooserEnvironment.Scope.INIT, AccumuloTable.METADATA.tableId(), null, context);
    String defaultMetadataTabletDirName =
        MetadataSchema.TabletsSection.ServerColumnFamily.DEFAULT_TABLET_DIR_NAME;
    String defaultMetadataTabletDirUri =
        fs.choose(chooserEnv, context.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR
            + AccumuloTable.METADATA.tableId() + Path.SEPARATOR + defaultMetadataTabletDirName;

    // create table and default tablets directories
    createDirectories(fs, rootTabletDirUri, defaultMetadataTabletDirUri);

    // populate the metadata tablet with info about scan ref tablets
    String metadataFileName = createScanRefTablet(context, fs);

    // populate the root tablet with info about the metadata table's two initial tablets
    Tablet tablesTablet = new Tablet(AccumuloTable.METADATA.tableId(), TABLE_TABLETS_TABLET_DIR,
        null, SPLIT_POINT, StoredTabletFile.of(new Path(metadataFileName)).getMetadata());
    Tablet defaultTablet = new Tablet(AccumuloTable.METADATA.tableId(),
        defaultMetadataTabletDirName, SPLIT_POINT, null);
    createMetadataFile(fs, rootTabletFileUri, tablesTablet, defaultTablet);
  }

  private void createDirectories(VolumeManager fs, String... dirs) throws IOException {
    for (String s : dirs) {
      Path dir = new Path(s);
      try {
        FileStatus fstat = fs.getFileStatus(dir);
        if (!fstat.isDirectory()) {
          log.error("FATAL: location {} exists but is not a directory", dir);
          return;
        }
      } catch (FileNotFoundException fnfe) {
        // attempt to create directory, since it doesn't exist
        if (!fs.mkdirs(dir)) {
          log.error("FATAL: unable to create directory {}", dir);
          return;
        }
      }
    }
  }

  private void initSystemTablesConfig(final ServerContext context)
      throws IOException, InterruptedException, KeeperException {
    setTableProperties(context, AccumuloTable.ROOT.tableId(), initConfig.getRootTableConf());
    setTableProperties(context, AccumuloTable.ROOT.tableId(), initConfig.getRootMetaConf());
    setTableProperties(context, AccumuloTable.METADATA.tableId(), initConfig.getRootMetaConf());
    setTableProperties(context, AccumuloTable.METADATA.tableId(), initConfig.getMetaTableConf());
  }

  private void setTableProperties(final ServerContext context, TableId tableId,
      HashMap<String,String> props) {
    var propStore = context.getPropStore();
    TablePropKey tablePropKey = TablePropKey.of(context, tableId);
    if (propStore.exists(tablePropKey)) {
      propStore.putAll(tablePropKey, props);
    } else {
      propStore.create(tablePropKey, props);
    }
  }

  private void createMetadataFile(VolumeManager volmanager, String fileName, Tablet... tablets)
      throws IOException {
    AccumuloConfiguration conf = initConfig.getSiteConf();
    // sort file contents in memory, then play back to the file
    TreeMap<Key,Value> sorted = new TreeMap<>();
    for (Tablet tablet : tablets) {
      createEntriesForTablet(sorted, tablet);
    }
    ReferencedTabletFile file = ReferencedTabletFile.of(new Path(fileName));
    FileSystem fs = volmanager.getFileSystemByPath(file.getPath());

    CryptoService cs = CryptoFactoryLoader.getServiceForServer(conf);

    FileSKVWriter tabletWriter = FileOperations.getInstance().newWriterBuilder()
        .forFile(file, fs, fs.getConf(), cs).withTableConfiguration(conf).build();
    tabletWriter.startDefaultLocalityGroup();

    for (Map.Entry<Key,Value> entry : sorted.entrySet()) {
      tabletWriter.append(entry.getKey(), entry.getValue());
    }

    tabletWriter.close();
  }

  private void createEntriesForTablet(TreeMap<Key,Value> map, Tablet tablet) {
    Value EMPTY_SIZE = new DataFileValue(0, 0).encodeAsValue();
    Text extent = new Text(MetadataSchema.TabletsSection.encodeRow(tablet.tableId, tablet.endRow));
    addEntry(map, extent, DIRECTORY_COLUMN, new Value(tablet.dirName));
    addEntry(map, extent, TIME_COLUMN, new Value(new MetadataTime(0, TimeType.LOGICAL).encode()));
    addEntry(map, extent, PREV_ROW_COLUMN,
        MetadataSchema.TabletsSection.TabletColumnFamily.encodePrevEndRow(tablet.prevEndRow));
    for (String file : tablet.files) {
      addEntry(map, extent,
          new ColumnFQ(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME, new Text(file)),
          EMPTY_SIZE);
    }
  }

  private void addEntry(TreeMap<Key,Value> map, Text row, ColumnFQ col, Value value) {
    map.put(new Key(row, col.getColumnFamily(), col.getColumnQualifier(), 0), value);
  }

  public String createScanRefTablet(ServerContext context, VolumeManager fs) throws IOException {
    // First set table properties in ZK
    setTableProperties(context, AccumuloTable.SCAN_REF.tableId(), initConfig.getScanRefTableConf());

    VolumeChooserEnvironment chooserEnv =
        new VolumeChooserEnvironmentImpl(VolumeChooserEnvironment.Scope.INIT,
            AccumuloTable.METADATA.tableId(), SPLIT_POINT, context);

    String tableMetadataTabletDirUri =
        fs.choose(chooserEnv, context.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR
            + AccumuloTable.METADATA.tableId() + Path.SEPARATOR + TABLE_TABLETS_TABLET_DIR;

    // Create Default Tablet entry for ScanServerRefTable
    chooserEnv = new VolumeChooserEnvironmentImpl(VolumeChooserEnvironment.Scope.INIT,
        AccumuloTable.SCAN_REF.tableId(), null, context);

    String scanRefTableDefaultTabletDirName =
        MetadataSchema.TabletsSection.ServerColumnFamily.DEFAULT_TABLET_DIR_NAME;
    String scanRefTableDefaultTabletDirUri =
        fs.choose(chooserEnv, context.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR
            + AccumuloTable.SCAN_REF.tableId() + Path.SEPARATOR + scanRefTableDefaultTabletDirName;

    createDirectories(fs, tableMetadataTabletDirUri, scanRefTableDefaultTabletDirUri);

    String ext = FileOperations.getNewFileExtension(DefaultConfiguration.getInstance());
    String metadataFileName = tableMetadataTabletDirUri + Path.SEPARATOR + "0.1." + ext;

    Tablet scanRefTablet =
        new Tablet(AccumuloTable.SCAN_REF.tableId(), scanRefTableDefaultTabletDirName, null, null);
    createMetadataFile(fs, metadataFileName, scanRefTablet);

    return metadataFileName;
  }
}
