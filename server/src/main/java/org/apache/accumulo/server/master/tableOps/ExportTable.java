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
package org.apache.accumulo.server.master.tableOps;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.master.Master;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

class ExportInfo implements Serializable {
  
  private static final long serialVersionUID = 1L;

  public String tableName;
  public String tableID;
  public String exportDir;
}

class WriteExportFiles extends MasterRepo {
  
  private static final long serialVersionUID = 1L;
  private final ExportInfo tableInfo;
  
  WriteExportFiles(ExportInfo tableInfo) {
    this.tableInfo = tableInfo;
  }

  private void checkOffline(Connector conn) throws Exception {
    if (Tables.getTableState(conn.getInstance(), tableInfo.tableID) != TableState.OFFLINE) {
      Tables.clearCache(conn.getInstance());
      if (Tables.getTableState(conn.getInstance(), tableInfo.tableID) != TableState.OFFLINE) {
        throw new ThriftTableOperationException(tableInfo.tableID, tableInfo.tableName, TableOperation.EXPORT, TableOperationExceptionType.OTHER,
            "Table is not offline");
      }
    }
  }
  
  @Override
  public long isReady(long tid, Master master) throws Exception {
    
    long reserved = Utils.reserveTable(tableInfo.tableID, tid, false, true, TableOperation.EXPORT);
    if (reserved > 0)
      return reserved;

    Connector conn = master.getConnector();
    
    checkOffline(conn);
    
    Scanner metaScanner = conn.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
    metaScanner.setRange(new KeyExtent(new Text(tableInfo.tableID), null, null).toMetadataRange());
    
    // scan for locations
    metaScanner.fetchColumnFamily(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY);
    metaScanner.fetchColumnFamily(Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY);
    
    if (metaScanner.iterator().hasNext()) {
      return 500;
    }
    
    // use the same range to check for walogs that we used to check for hosted (or future hosted) tablets
    // this is done as a separate scan after we check for locations, because walogs are okay only if there is no location
    metaScanner.clearColumns();
    metaScanner.fetchColumnFamily(Constants.METADATA_LOG_COLUMN_FAMILY);
    
    if (metaScanner.iterator().hasNext()) {
      throw new ThriftTableOperationException(tableInfo.tableID, tableInfo.tableName, TableOperation.EXPORT, TableOperationExceptionType.OTHER,
          "Write ahead logs found for table");
    }

    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    Connector conn = master.getConnector();

    try {
      exportTable(master.getFileSystem(), conn, tableInfo.tableName, tableInfo.tableID, tableInfo.exportDir);
    } catch (IOException ioe) {
      throw new ThriftTableOperationException(tableInfo.tableID, tableInfo.tableName, TableOperation.EXPORT, TableOperationExceptionType.OTHER,
          "Failed to create export files " + ioe.getMessage());
    }
    Utils.unreserveTable(tableInfo.tableID, tid, false);
    Utils.unreserveHdfsDirectory(new Path(tableInfo.exportDir).toString(), tid);
    return null;
  }
  
  @Override
  public void undo(long tid, Master env) throws Exception {
    Utils.unreserveTable(tableInfo.tableID, tid, false);
  }
  
  public static void exportTable(FileSystem fs, Connector conn, String tableName, String tableID, String exportDir) throws Exception {

    fs.mkdirs(new Path(exportDir));
    
    Path exportMetaFilePath = new Path(exportDir, Constants.EXPORT_FILE);
    
    FSDataOutputStream fileOut = fs.create(exportMetaFilePath, false);
    ZipOutputStream zipOut = new ZipOutputStream(fileOut);
    BufferedOutputStream bufOut = new BufferedOutputStream(zipOut);
    DataOutputStream dataOut = new DataOutputStream(bufOut);
    
    try {
      
      zipOut.putNextEntry(new ZipEntry(Constants.EXPORT_INFO_FILE));
      OutputStreamWriter osw = new OutputStreamWriter(dataOut);
      osw.append(ExportTable.EXPORT_VERSION_PROP + ":" + ExportTable.VERSION + "\n");
      osw.append("srcInstanceName:" + conn.getInstance().getInstanceName() + "\n");
      osw.append("srcInstanceID:" + conn.getInstance().getInstanceID() + "\n");
      osw.append("srcZookeepers:" + conn.getInstance().getZooKeepers() + "\n");
      osw.append("srcTableName:" + tableName + "\n");
      osw.append("srcTableID:" + tableID + "\n");
      osw.append(ExportTable.DATA_VERSION_PROP + ":" + Constants.DATA_VERSION + "\n");
      osw.append("srcCodeVersion:" + Constants.VERSION + "\n");
      
      osw.flush();
      dataOut.flush();
      
      exportConfig(conn, tableID, zipOut, dataOut);
      dataOut.flush();
      
      Map<String,String> uniqueFiles = exportMetadata(conn, tableID, zipOut, dataOut);
      
      dataOut.close();
      dataOut = null;

      createDistcpFile(fs, exportDir, exportMetaFilePath, uniqueFiles);
      
    } finally {
      if (dataOut != null)
        dataOut.close();
    }
  }

  private static void createDistcpFile(FileSystem fs, String exportDir, Path exportMetaFilePath, Map<String,String> uniqueFiles) throws IOException {
    BufferedWriter distcpOut = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(exportDir, "distcp.txt"), false)));
    
    try {
      URI uri = fs.getUri();
      
      for (String relPath : uniqueFiles.values()) {
        Path absPath = new Path(uri.getScheme(), uri.getAuthority(), ServerConstants.getTablesDir() + relPath);
        distcpOut.append(absPath.toUri().toString());
        distcpOut.newLine();
      }
      
      Path absEMP = exportMetaFilePath;
      if (!exportMetaFilePath.isAbsolute())
        absEMP = new Path(fs.getWorkingDirectory().toUri().getPath(), exportMetaFilePath);
      
      distcpOut.append(new Path(uri.getScheme(), uri.getAuthority(), absEMP.toString()).toUri().toString());

      distcpOut.newLine();
      
      distcpOut.close();
      distcpOut = null;

    } finally {
      if (distcpOut != null)
        distcpOut.close();
    }
  }
  
  private static Map<String,String> exportMetadata(Connector conn, String tableID, ZipOutputStream zipOut, DataOutputStream dataOut) throws IOException,
      TableNotFoundException {
    zipOut.putNextEntry(new ZipEntry(Constants.EXPORT_METADATA_FILE));
    
    Map<String,String> uniqueFiles = new HashMap<String,String>();

    Scanner metaScanner = conn.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
    metaScanner.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
    Constants.METADATA_PREV_ROW_COLUMN.fetch(metaScanner);
    Constants.METADATA_TIME_COLUMN.fetch(metaScanner);
    metaScanner.setRange(new KeyExtent(new Text(tableID), null, null).toMetadataRange());
    
    for (Entry<Key,Value> entry : metaScanner) {
      entry.getKey().write(dataOut);
      entry.getValue().write(dataOut);
      
      if (entry.getKey().getColumnFamily().equals(Constants.METADATA_DATAFILE_COLUMN_FAMILY)) {
        String relPath = entry.getKey().getColumnQualifierData().toString();
        
        if (relPath.startsWith("../"))
          relPath = relPath.substring(2);
        else
          relPath = "/" + tableID + relPath;
        
        String tokens[] = relPath.split("/");
        if (tokens.length != 4) {
          throw new RuntimeException("Illegal path " + relPath);
        }
        
        String filename = tokens[3];
        
        String existingPath = uniqueFiles.get(filename);
        if (existingPath == null) {
          uniqueFiles.put(filename, relPath);
        } else if (!existingPath.equals(relPath)) {
          // make sure file names are unique, should only apply for tables with file names generated by Accumulo 1.3 and earlier
          // TODO throw another type of exception?
          throw new RuntimeException("Cannot export table with nonunique file names " + filename + ". Major compact table.");
        }
        
      }
    }
    return uniqueFiles;
  }

  private static void exportConfig(Connector conn, String tableID, ZipOutputStream zipOut, DataOutputStream dataOut) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException, IOException {

    DefaultConfiguration defaultConfig = AccumuloConfiguration.getDefaultConfiguration();
    Map<String,String> siteConfig = conn.instanceOperations().getSiteConfiguration();
    Map<String,String> systemConfig = conn.instanceOperations().getSystemConfiguration();

    TableConfiguration tableConfig = ServerConfiguration.getTableConfiguration(conn.getInstance(), tableID);

    OutputStreamWriter osw = new OutputStreamWriter(dataOut);

    // only put props that are different than defaults and higher level configurations
    zipOut.putNextEntry(new ZipEntry(Constants.EXPORT_TABLE_CONFIG_FILE));
    for (Entry<String,String> prop : tableConfig) {
      if (prop.getKey().startsWith(Property.TABLE_PREFIX.getKey())) {
        Property key = Property.getPropertyByKey(prop.getKey());
        
        if (key == null || !defaultConfig.get(key).equals(prop.getValue())) {
          if (!prop.getValue().equals(siteConfig.get(prop.getKey())) && !prop.getValue().equals(systemConfig.get(prop.getKey()))) {
            osw.append(prop.getKey() + "=" + prop.getValue() + "\n");
          }
        }
      }
    }
    
    osw.flush();
  }
}

public class ExportTable extends MasterRepo {
  private static final long serialVersionUID = 1L;
  
  private final ExportInfo tableInfo;

  public ExportTable(String tableName, String tableId, String exportDir) {
    tableInfo = new ExportInfo();
    tableInfo.tableName = tableName;
    tableInfo.exportDir = exportDir;
    tableInfo.tableID = tableId;
  }
  
  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return Utils.reserveHdfsDirectory(new Path(tableInfo.exportDir).toString(), tid);
  }
  
  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    return new WriteExportFiles(tableInfo);
  }
  
  @Override
  public void undo(long tid, Master env) throws Exception {
    Utils.unreserveHdfsDirectory(new Path(tableInfo.exportDir).toString(), tid);
  }
  
  public static final int VERSION = 1;
  
  public static final String DATA_VERSION_PROP = "srcDataVersion";
  public static final String EXPORT_VERSION_PROP = "exportVersion";

}
